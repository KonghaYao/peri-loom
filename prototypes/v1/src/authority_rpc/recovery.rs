//! Authority-side recovery state machine.
//!
//! The launcher owns proof-based rollback for an interrupted startup. This
//! module covers the other side of the boundary: an authority process that was
//! killed after readiness was announced, and therefore may have executed SQL
//! whose outcome is unknown. The state machine never replays work. It emits a
//! machine-checkable plan, and only a plan whose evidence reconciles the old
//! process, the old authority term, the backend terminal and the operation view
//! may start the recovery instance. That instance admits only inspection and
//! shutdown commands until an operator stops it cleanly and restarts normally.

use std::collections::BTreeSet;

use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};

use super::protocol::AuthorityCommand;

pub const RECOVERY_EVIDENCE_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackendKind {
    Local,
    S3,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Readiness {
    /// The launcher recorded that no service announced readiness before the
    /// crash. Only the launcher can turn this into a rollback decision.
    NotAnnounced,
    /// Readiness was announced, so SQL may have been admitted and committed.
    Announced,
    /// No trustworthy readiness record exists. This is not proof that SQL did
    /// not run; it must fail closed.
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarkerState {
    Clean,
    Dirty,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreviousAuthority {
    /// Seal owner written by the authority process that may have crashed.
    pub owner: String,
    /// Seal etag that owner last published. The recovery takeover must CAS on
    /// this exact etag, so a late old authority write cannot silently win.
    pub etag: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalProcessEvidence {
    /// The launcher observed the exact recorded process join (not a PID probe).
    pub joined: bool,
    pub pid: u32,
    /// SHA-256 of the exact recorded argv used to match the process table.
    pub argv_sha256: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteTerminalEvidence {
    /// The old seal owner/etag the evidence is bound to. Both must equal
    /// `previous_authority`; a local PID record cannot stand in for this.
    pub owner: String,
    pub etag: String,
    /// Provider-observed read-back after the old writer was fenced.
    pub readback_etag: String,
    pub readback_sha256: String,
    pub observed_at: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct OperationsEvidence {
    pub journal_sha256: String,
    pub terminal_sha256: String,
    pub audit_head: String,
    pub audit_verified: bool,
    /// The launcher read back every acknowledged successful write from the
    /// recovered database. Unknown outcomes are listed separately and are not
    /// inferred from a timeout, close or read-back.
    #[serde(default)]
    pub readback_verified: bool,
    pub operations_checkpoint: u64,
    pub floor_revision: u64,
    pub root_revision: u64,
    #[serde(default)]
    pub known: Vec<String>,
    #[serde(default)]
    pub unknown: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreservedSnapshot {
    pub intent_sha256: String,
    pub terminal_sha256: String,
    pub state_sha256: String,
    pub state_floor_sha256: String,
    pub journal_sha256: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryEvidence {
    pub version: u32,
    pub generation: String,
    pub backend: BackendKind,
    pub readiness: Readiness,
    #[serde(default)]
    pub previous_authority: Option<PreviousAuthority>,
    #[serde(default)]
    pub local_process: Option<LocalProcessEvidence>,
    #[serde(default)]
    pub remote_terminal: Option<RemoteTerminalEvidence>,
    pub operations: OperationsEvidence,
    #[serde(default)]
    pub preserved_snapshot: Option<PreservedSnapshot>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryPath {
    Clean,
    LegacyReconciled,
    InterruptedStartup,
    PostReadyCrash,
    Unclassified,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StepOutcome {
    Passed,
    Failed,
    NotApplicable,
}

#[derive(Clone, Debug, Serialize)]
pub struct RecoveryStep {
    pub id: String,
    pub evidence: String,
    pub outcome: StepOutcome,
    pub detail: String,
    pub operator_action: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "decision", rename_all = "snake_case")]
pub enum RecoveryDecision {
    StartNormal,
    StartRestricted { pending_unknown: Vec<String> },
    LauncherRollbackRequired { actions: Vec<String> },
    OperatorRequired { actions: Vec<String> },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplayPolicy {
    /// Recovery never re-executes an operation, known or unknown.
    Never,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct SealExpectation {
    owner: String,
    etag: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct RecoveryPlan {
    version: u32,
    path: RecoveryPath,
    backend: BackendKind,
    decision: RecoveryDecision,
    replay_policy: ReplayPolicy,
    steps: Vec<RecoveryStep>,
    operator_actions: Vec<String>,
    seal_expectation: Option<SealExpectation>,
    seal_verified: Option<bool>,
}

impl RecoveryPlan {
    pub fn admission(&self) -> RecoveryAdmission {
        match &self.decision {
            RecoveryDecision::StartRestricted { pending_unknown } => {
                RecoveryAdmission::Restricted {
                    pending_unknown: pending_unknown.clone(),
                }
            }
            RecoveryDecision::StartNormal
            | RecoveryDecision::LauncherRollbackRequired { .. }
            | RecoveryDecision::OperatorRequired { .. } => RecoveryAdmission::Normal,
        }
    }

    pub fn may_start(&self) -> bool {
        matches!(
            self.decision,
            RecoveryDecision::StartNormal | RecoveryDecision::StartRestricted { .. }
        )
    }

    pub fn decision(&self) -> &RecoveryDecision {
        &self.decision
    }

    pub fn path(&self) -> RecoveryPath {
        self.path
    }

    pub(crate) fn record_seal_verified(&mut self) {
        self.seal_verified = Some(true);
    }

    pub(crate) fn authorize_seal_takeover(
        &self,
        owner: &str,
        etag: &str,
        clean: bool,
    ) -> Result<()> {
        if clean {
            return Ok(());
        }
        ensure!(
            matches!(self.decision, RecoveryDecision::StartRestricted { .. })
                && self.path == RecoveryPath::PostReadyCrash,
            "dirty backend authority seal; {}",
            self.refusal_message()
        );
        let expected = self
            .seal_expectation
            .as_ref()
            .context("dirty backend authority seal has no recovery evidence")?;
        ensure!(
            expected.owner == owner && expected.etag == etag,
            "authority seal owner/etag changed after the recovery evidence was produced"
        );
        Ok(())
    }

    pub(crate) fn seal_expectation(&self) -> Option<(&str, &str)> {
        self.seal_expectation
            .as_ref()
            .map(|expected| (expected.owner.as_str(), expected.etag.as_str()))
    }

    pub fn refusal_message(&self) -> String {
        let failed = self
            .steps
            .iter()
            .filter(|step| step.outcome == StepOutcome::Failed)
            .map(|step| format!("{} ({})", step.id, step.detail))
            .collect::<Vec<_>>();
        let mut message = format!(
            "authority recovery {}: path={:?}",
            decision_label(&self.decision),
            self.path
        );
        if !failed.is_empty() {
            message.push_str("; failed: ");
            message.push_str(&failed.join("; "));
        }
        if !self.operator_actions.is_empty() {
            message.push_str("; operator: ");
            message.push_str(&self.operator_actions.join("; "));
        }
        message
    }

    pub fn to_json_pretty(&self) -> Result<String> {
        Ok(serde_json::to_string_pretty(self)?)
    }
}

fn decision_label(decision: &RecoveryDecision) -> &'static str {
    match decision {
        RecoveryDecision::StartNormal => "start_normal",
        RecoveryDecision::StartRestricted { .. } => "start_restricted",
        RecoveryDecision::LauncherRollbackRequired { .. } => "launcher_rollback_required",
        RecoveryDecision::OperatorRequired { .. } => "operator_required",
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum RecoveryAdmission {
    Normal,
    Restricted { pending_unknown: Vec<String> },
}

impl RecoveryAdmission {
    pub fn is_restricted(&self) -> bool {
        matches!(self, Self::Restricted { .. })
    }

    pub fn pending_unknown(&self) -> &[String] {
        match self {
            Self::Normal => &[],
            Self::Restricted { pending_unknown } => pending_unknown,
        }
    }

    /// The restricted profile exists to inspect and stop a recovered state. It
    /// never admits a command that can create a session, grant a lease, mutate
    /// the catalog or run a session operation.
    pub fn admits(&self, command: &AuthorityCommand) -> bool {
        if !self.is_restricted() {
            return true;
        }
        matches!(
            command,
            AuthorityCommand::GetMetrics
                | AuthorityCommand::GetCapacityTrace { .. }
                | AuthorityCommand::GetDatabaseState { .. }
                | AuthorityCommand::ValidateLease { .. }
                | AuthorityCommand::SessionState { .. }
                | AuthorityCommand::CloseSession { .. }
                | AuthorityCommand::RetireSession { .. }
                | AuthorityCommand::Shutdown
                | AuthorityCommand::ShutdownWithin { .. }
        )
    }
}

#[derive(Default)]
struct PlanBuilder {
    steps: Vec<RecoveryStep>,
    actions: Vec<String>,
    pending_unknown: Vec<String>,
    failed: bool,
}

impl PlanBuilder {
    fn passed(&mut self, id: &str, evidence: &str, detail: impl Into<String>) {
        self.push(id, evidence, StepOutcome::Passed, detail.into(), None);
    }

    fn not_applicable(&mut self, id: &str, evidence: &str, detail: impl Into<String>) {
        self.push(
            id,
            evidence,
            StepOutcome::NotApplicable,
            detail.into(),
            None,
        );
    }

    fn failed(&mut self, id: &str, evidence: &str, detail: impl Into<String>, action: &str) {
        self.failed = true;
        self.note(action);
        self.push(
            id,
            evidence,
            StepOutcome::Failed,
            detail.into(),
            Some(action.to_owned()),
        );
    }

    fn note(&mut self, action: &str) {
        if !self.actions.iter().any(|existing| existing == action) {
            self.actions.push(action.to_owned());
        }
    }

    fn push(
        &mut self,
        id: &str,
        evidence: &str,
        outcome: StepOutcome,
        detail: String,
        operator_action: Option<String>,
    ) {
        self.steps.push(RecoveryStep {
            id: id.to_owned(),
            evidence: evidence.to_owned(),
            outcome,
            detail,
            operator_action,
        });
    }
}

/// Evaluate the durable facts available to the authority against the
/// launcher/operator attestation. All required steps are evaluated even after
/// one fails, so the emitted plan tells the operator everything that is
/// missing. The result never authorizes a replay.
pub fn evaluate(
    marker: MarkerState,
    backend: BackendKind,
    evidence: Option<&RecoveryEvidence>,
    confirmed_quiescence: bool,
    state_dir_exclusive: bool,
) -> RecoveryPlan {
    let mut plan = PlanBuilder::default();
    let path;
    let mut seal_expectation = None;

    match marker {
        MarkerState::Clean => {
            path = RecoveryPath::Clean;
            plan.passed(
                "startup_marker",
                "authority.lock bytes",
                "marker=clean; the previous authority completed its shutdown path",
            );
            if state_dir_exclusive {
                plan.passed(
                    "state_dir_exclusive",
                    "exclusive authority.lock",
                    "state directory is exclusively owned by this process",
                );
            } else {
                plan.failed(
                    "state_dir_exclusive",
                    "exclusive authority.lock",
                    "the state directory is not exclusively held",
                    "stop the process that owns the state directory and retry",
                );
            }
            match evidence {
                Some(evidence) => {
                    check_header(&mut plan, evidence, backend);
                    plan.not_applicable(
                        "readiness",
                        "marker=clean",
                        "a clean marker already covers shutdown; readiness does not add a recovery decision",
                    );
                    plan.not_applicable(
                        "old_process_stopped",
                        "marker=clean",
                        "the clean shutdown marker and the exclusive lock cover the old local process",
                    );
                    plan.not_applicable(
                        "backend_terminal",
                        "marker=clean",
                        "the clean seal path remains authoritative",
                    );
                    plan.not_applicable(
                        "operations_reconciled",
                        "marker=clean",
                        "operation reconciliation belongs to the normal platform gate",
                    );
                }
                None => {
                    plan.not_applicable(
                        "recovery_evidence",
                        "no evidence file",
                        "a clean marker needs no post-ready evidence",
                    );
                }
            }
        }
        MarkerState::Dirty => {
            plan.passed(
                "startup_marker",
                "authority.lock bytes",
                "marker=dirty; the previous authority did not complete its shutdown path",
            );
            if state_dir_exclusive {
                plan.passed(
                    "state_dir_exclusive",
                    "exclusive authority.lock",
                    "the old state-directory owner is gone; this does not by itself prove a remote terminal",
                );
            } else {
                plan.failed(
                    "state_dir_exclusive",
                    "exclusive authority.lock",
                    "the state directory is not exclusively held",
                    "stop the process that owns the state directory and retry",
                );
            }
            match evidence {
                None if confirmed_quiescence => {
                    path = RecoveryPath::LegacyReconciled;
                    plan.passed(
                        "operator_confirmation",
                        "--reconciled",
                        "operator confirmed old backend operations quiescent; this is not a proof-based recovery and never rewrites the seal",
                    );
                    plan.not_applicable(
                        "recovery_evidence",
                        "no evidence file",
                        "legacy explicit confirmation path",
                    );
                }
                None => {
                    path = RecoveryPath::Unclassified;
                    plan.failed(
                        "recovery_evidence",
                        "no evidence file and no --reconciled",
                        "dirty marker without a readiness record or stop proof",
                        "provide --recovery-evidence from the launcher/operator, or stop and join every service and confirm they are quiescent with --reconciled; never delete or rewrite the marker",
                    );
                }
                Some(evidence) => {
                    check_header(&mut plan, evidence, backend);
                    match evidence.readiness {
                        Readiness::NotAnnounced => {
                            path = RecoveryPath::InterruptedStartup;
                            plan.passed(
                                "readiness",
                                "launcher readiness record",
                                "readiness was not announced; the launcher startup-rollback path owns this crash",
                            );
                            check_preserved_snapshot(&mut plan, evidence);
                            plan.note(
                                "run the launcher with --recover so the signed startup intent and preserved snapshot can restore the last clean point; the authority must not roll back a dirty marker itself",
                            );
                            if backend == BackendKind::S3 {
                                plan.note(
                                    "S3 has no proof-based startup rollback; keep the volume isolated and resolve the S3 authority seal manually",
                                );
                            }
                        }
                        Readiness::Announced => {
                            path = RecoveryPath::PostReadyCrash;
                            plan.passed(
                                "readiness",
                                "launcher readiness record",
                                "readiness was announced; SQL may have been admitted, so a startup rollback is forbidden",
                            );
                            check_post_ready(&mut plan, evidence, backend, &mut seal_expectation);
                        }
                        Readiness::Unknown => {
                            path = RecoveryPath::Unclassified;
                            plan.failed(
                                "readiness",
                                "launcher readiness record",
                                "readiness is unknown; absence of a record is not proof that SQL did not run",
                                "recover the signed readiness record from the launcher; do not guess that the crash happened before readiness",
                            );
                            check_post_ready(&mut plan, evidence, backend, &mut seal_expectation);
                        }
                    }
                }
            }
        }
    }

    let decision = if plan.failed {
        RecoveryDecision::OperatorRequired {
            actions: plan.actions.clone(),
        }
    } else {
        match path {
            RecoveryPath::Clean | RecoveryPath::LegacyReconciled => RecoveryDecision::StartNormal,
            RecoveryPath::PostReadyCrash => RecoveryDecision::StartRestricted {
                pending_unknown: plan.pending_unknown.clone(),
            },
            RecoveryPath::InterruptedStartup => RecoveryDecision::LauncherRollbackRequired {
                actions: plan.actions.clone(),
            },
            RecoveryPath::Unclassified => RecoveryDecision::OperatorRequired {
                actions: plan.actions.clone(),
            },
        }
    };

    RecoveryPlan {
        version: RECOVERY_EVIDENCE_VERSION,
        path,
        backend,
        decision,
        replay_policy: ReplayPolicy::Never,
        steps: plan.steps,
        operator_actions: plan.actions,
        seal_expectation,
        seal_verified: None,
    }
}

fn check_header(plan: &mut PlanBuilder, evidence: &RecoveryEvidence, backend: BackendKind) -> bool {
    let mut ok = true;
    if evidence.version != RECOVERY_EVIDENCE_VERSION {
        ok = false;
        plan.failed(
            "evidence_version",
            "recovery evidence version",
            format!("unsupported evidence version {}", evidence.version),
            "regenerate the recovery evidence with the matching launcher/authority version",
        );
    }
    if evidence.generation.trim().is_empty() {
        ok = false;
        plan.failed(
            "evidence_generation",
            "recovery evidence generation",
            "generation is empty",
            "provide the signed run generation in the recovery evidence",
        );
    }
    if evidence.backend != backend {
        ok = false;
        plan.failed(
            "backend_identity",
            "recovery evidence backend",
            format!(
                "evidence backend {:?} does not match the authority backend {:?}",
                evidence.backend, backend
            ),
            "re-run the authority against the backend named in the evidence; never reuse local process proof for a remote backend",
        );
    }
    ok
}

fn check_preserved_snapshot(plan: &mut PlanBuilder, evidence: &RecoveryEvidence) {
    match &evidence.preserved_snapshot {
        Some(snapshot)
            if valid_digest(&snapshot.intent_sha256)
                && valid_digest(&snapshot.terminal_sha256)
                && valid_digest(&snapshot.state_sha256)
                && valid_digest(&snapshot.state_floor_sha256)
                && valid_digest(&snapshot.journal_sha256) =>
        {
            plan.passed(
                "preserved_snapshot",
                "signed launcher startup intent and preserved terminal/stop/state/floor/journal",
                "the preserved snapshot is complete and bound to digests",
            );
        }
        Some(_) => {
            plan.failed(
                "preserved_snapshot",
                "signed launcher startup intent and preserved terminal/stop/state/floor/journal",
                "the preserved snapshot is incomplete or has malformed digests",
                "retain the preserved launcher-startup-attempt.json and launcher-restart-staging directory; rebuild only if the original clean point cannot be reconstructed",
            );
        }
        None => {
            plan.failed(
                "preserved_snapshot",
                "signed launcher startup intent and preserved terminal/stop/state/floor/journal",
                "no preserved snapshot was provided",
                "run the launcher --recover with the signed startup intent; do not start the authority from a dirty marker without it",
            );
        }
    }
}

fn check_post_ready(
    plan: &mut PlanBuilder,
    evidence: &RecoveryEvidence,
    backend: BackendKind,
    seal_expectation: &mut Option<SealExpectation>,
) {
    let operations = &evidence.operations;
    if valid_digest(&operations.journal_sha256) && valid_digest(&operations.terminal_sha256) {
        plan.passed(
            "recovery_journal",
            "operation journal and terminal digests",
            "the recovery journal and the terminal account are present and bound by digests",
        );
    } else {
        plan.failed(
            "recovery_journal",
            "operation journal and terminal digests",
            "the operation journal or terminal digest is missing or malformed",
            "stop the platform and authority gracefully so the shutdown checkpoint materializes the operation view; retain all evidence and do not replay SQL",
        );
    }

    if operations.audit_verified && valid_digest(&operations.audit_head) {
        plan.passed(
            "audit_reconciled",
            "audit head and launcher verification result",
            "the launcher verified the audit chain at the recorded head",
        );
    } else {
        plan.failed(
            "audit_reconciled",
            "audit head and launcher verification result",
            "the audit chain was not verified or its head is missing or malformed",
            "verify the audit chain from the preserved/terminal evidence before opening the recovered state; never create or replay an audit append",
        );
    }

    if operations.readback_verified {
        plan.passed(
            "confirmed_writes_readable",
            "launcher read-back of acknowledged writes",
            "every confirmed successful write was read back from the recovered state",
        );
    } else {
        plan.failed(
            "confirmed_writes_readable",
            "launcher read-back of acknowledged writes",
            "acknowledged writes were not read back",
            "read back every confirmed successful write from the recovered database; an unknown outcome must remain pending and must never be inferred from a timeout, close or read-back",
        );
    }

    match validate_operation_ids(operations) {
        Ok(unknown) => {
            if unknown.is_empty() {
                plan.passed(
                    "operations_reconciled",
                    "known/unknown operation lists",
                    format!(
                        "{} known operations; no unknown operations remain",
                        operations.known.len()
                    ),
                );
            } else {
                plan.pending_unknown = unknown.clone();
                plan.passed(
                    "operations_reconciled",
                    "known/unknown operation lists",
                    format!(
                        "{} unknown operations remain pending ({})",
                        unknown.len(),
                        unknown.join(",")
                    ),
                );
                plan.note(
                    "unknown SQL stays pending: inspect the operation records, do not re-execute them; stop the restricted instance cleanly and restart normally only after the operator resolves or explicitly quarantines each unknown outcome",
                );
            }
        }
        Err(reason) => {
            plan.failed(
                "operations_reconciled",
                "known/unknown operation lists",
                reason,
                "repair the operation view from the signed terminal evidence; do not infer success from a timeout, close or read-back",
            );
        }
    }

    if operations.floor_revision > 0 && operations.root_revision > 0 {
        plan.passed(
            "floor_and_root",
            "platform state floor and authority root revisions",
            format!(
                "floor_revision={} root_revision={} operations_checkpoint={}",
                operations.floor_revision,
                operations.root_revision,
                operations.operations_checkpoint
            ),
        );
    } else {
        plan.failed(
            "floor_and_root",
            "platform state floor and authority root revisions",
            "the state floor or authority root revision is not materialized",
            "stop the platform and authority gracefully so the shutdown checkpoint materializes the operation view; rebuild only if it remains absent after a joined stop",
        );
    }

    match &evidence.previous_authority {
        Some(previous) if !previous.owner.trim().is_empty() && !previous.etag.trim().is_empty() => {
            plan.passed(
                "old_authority_term",
                "seal owner and etag recorded before the crash",
                format!(
                    "old owner is bound to the evidence; a takeover CAS must match etag {}",
                    previous.etag
                ),
            );
            *seal_expectation = Some(SealExpectation {
                owner: previous.owner.clone(),
                etag: previous.etag.clone(),
            });
        }
        Some(_) => {
            plan.failed(
                "old_authority_term",
                "seal owner and etag recorded before the crash",
                "the previous authority owner or etag is empty",
                "recover the seal owner/etag from the preserved terminal or the backend; do not overwrite the seal blindly",
            );
        }
        None => {
            plan.failed(
                "old_authority_term",
                "seal owner and etag recorded before the crash",
                "the previous authority identity is missing",
                "recover the old seal owner/etag; without it the new authority cannot prove which term it is fencing",
            );
        }
    }

    let process_ok = match &evidence.local_process {
        Some(process)
            if process.joined && process.pid > 0 && valid_digest(&process.argv_sha256) =>
        {
            plan.passed(
                "old_process_stopped",
                "launcher-observed joined process, exact recorded argv digest, exclusive state-directory lock",
                format!(
                    "pid {} joined the launcher stop; exact argv is bound by its digest",
                    process.pid
                ),
            );
            true
        }
        Some(_) => {
            plan.failed(
                "old_process_stopped",
                "launcher-observed joined process, exact recorded argv digest, exclusive state-directory lock",
                "the old process is not joined or its identity digest is malformed",
                "send SIGTERM to the surviving authority and wait for the recorded process to join; never use a timeout or SIGKILL as a clean stop proof",
            );
            false
        }
        None => {
            plan.failed(
                "old_process_stopped",
                "launcher-observed joined process, exact recorded argv digest, exclusive state-directory lock",
                "the launcher did not record a joined old process",
                "stop and join the old authority with the launcher, retain its exact argv/PID evidence, then retry",
            );
            false
        }
    };

    match backend {
        BackendKind::Local if process_ok => {
            plan.passed(
                "backend_terminal",
                "local backend: exclusive state-directory lock and joined old process",
                "no old local process can still write this volume; local process proof is not reused for a remote backend",
            );
        }
        BackendKind::Local => {
            plan.failed(
                "backend_terminal",
                "local backend: exclusive state-directory lock and joined old process",
                "the local old process is not proven stopped",
                "stop and join the old authority before starting a local recovery",
            );
        }
        BackendKind::S3 => {
            let previous = evidence.previous_authority.as_ref();
            match &evidence.remote_terminal {
                Some(remote)
                    if previous.is_some_and(|previous| {
                        remote.owner == previous.owner
                            && remote.etag == previous.etag
                            && remote.readback_etag == previous.etag
                    }) && valid_digest(&remote.readback_sha256)
                        && !remote.observed_at.trim().is_empty() =>
                {
                    plan.passed(
                        "backend_terminal",
                        "provider read-back bound to the old seal owner/etag",
                        format!(
                            "remote terminal read back at etag {} at {}",
                            remote.readback_etag, remote.observed_at
                        ),
                    );
                }
                Some(_) => {
                    plan.failed(
                        "backend_terminal",
                        "provider read-back bound to the old seal owner/etag",
                        "the remote terminal evidence does not match the old authority or is malformed",
                        "obtain a provider read-back for the exact old seal owner/etag; a local PID proof is not accepted for remote late requests",
                    );
                }
                None => {
                    plan.failed(
                        "backend_terminal",
                        "provider read-back bound to the old seal owner/etag",
                        "no remote terminal evidence was provided",
                        "keep the S3 volume isolated and obtain provider-side evidence that the old authority term is fenced; do not reuse local process proof for the remote backend",
                    );
                }
            }
        }
    }
}

fn validate_operation_ids(
    operations: &OperationsEvidence,
) -> std::result::Result<Vec<String>, String> {
    let mut known = BTreeSet::new();
    for identifier in &operations.known {
        if identifier.trim().is_empty() {
            return Err("a known operation identifier is empty".into());
        }
        if !known.insert(identifier.as_str()) {
            return Err(format!("known operation {identifier} is duplicated"));
        }
    }
    let mut unknown = BTreeSet::new();
    for identifier in &operations.unknown {
        if identifier.trim().is_empty() {
            return Err("an unknown operation identifier is empty".into());
        }
        if !unknown.insert(identifier.as_str()) {
            return Err(format!("unknown operation {identifier} is duplicated"));
        }
        if known.contains(identifier.as_str()) {
            return Err(format!(
                "operation {identifier} is listed as both known and unknown"
            ));
        }
    }
    Ok(operations.unknown.clone())
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::authority::ownership::OwnershipAuthority;
    use crate::authority_rpc::auth::AuthConfig;
    use crate::authority_rpc::server::Server;
    use crate::authority_rpc::startup::AuthorityStartup;
    use crate::object_store::{MemoryObjectStore, ObjectStore};
    use crate::ownership::types::{AccessRole, LeaseBinding};
    use crate::ownership::SystemLeaseClock;
    use crate::runtime::DatabaseId;

    fn digest(seed: char) -> String {
        seed.to_string().repeat(64)
    }

    fn evidence(backend: BackendKind, readiness: Readiness) -> RecoveryEvidence {
        RecoveryEvidence {
            version: 1,
            generation: "generation-1".into(),
            backend,
            readiness,
            previous_authority: Some(PreviousAuthority {
                owner: "old-owner".into(),
                etag: "old-etag".into(),
            }),
            local_process: Some(LocalProcessEvidence {
                joined: true,
                pid: 4242,
                argv_sha256: digest('a'),
            }),
            remote_terminal: (backend == BackendKind::S3).then(|| RemoteTerminalEvidence {
                owner: "old-owner".into(),
                etag: "old-etag".into(),
                readback_etag: "old-etag".into(),
                readback_sha256: digest('b'),
                observed_at: "2026-10-20T00:00:00Z".into(),
            }),
            operations: OperationsEvidence {
                journal_sha256: digest('c'),
                terminal_sha256: digest('d'),
                audit_head: digest('e'),
                audit_verified: true,
                readback_verified: true,
                operations_checkpoint: 7,
                floor_revision: 7,
                root_revision: 9,
                known: vec!["op-known".into()],
                unknown: vec!["op-unknown".into()],
            },
            preserved_snapshot: None,
        }
    }

    fn dirty_marker(directory: &std::path::Path) {
        std::fs::write(directory.join("authority.lock"), b"dirty\n").unwrap();
    }

    fn prepare_directory() -> tempfile::TempDir {
        let directory = tempfile::tempdir().unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700))
                .unwrap();
        }
        directory
    }

    fn seed_dirty_seal(store: &Arc<dyn ObjectStore>) -> (super::super::seal::Seal, String, String) {
        let seal = super::super::seal::Seal::acquire(store.clone()).unwrap();
        let observation = super::super::seal::Seal::observe(store.as_ref())
            .unwrap()
            .expect("seeded seal");
        assert!(!observation.clean);
        (seal, observation.owner, observation.etag)
    }

    #[test]
    fn pre_ready_without_preserved_snapshot_requires_operator() -> Result<()> {
        let evidence = evidence(BackendKind::Local, Readiness::NotAnnounced);
        let plan = evaluate(
            MarkerState::Dirty,
            BackendKind::Local,
            Some(&evidence),
            false,
            true,
        );
        assert!(!plan.may_start());
        assert!(matches!(
            plan.decision(),
            RecoveryDecision::OperatorRequired { .. }
        ));
        assert!(plan.refusal_message().contains("preserved_snapshot"));
        Ok(())
    }

    #[test]
    fn state_directory_lock_is_required() -> Result<()> {
        let plan = evaluate(MarkerState::Clean, BackendKind::Local, None, false, false);
        assert!(!plan.may_start());
        assert!(plan.refusal_message().contains("state_dir_exclusive"));
        Ok(())
    }

    #[test]
    fn post_ready_requires_audit_readback_and_materialized_revisions() -> Result<()> {
        let mut evidence = evidence(BackendKind::Local, Readiness::Announced);
        evidence.operations.audit_verified = false;
        evidence.operations.readback_verified = false;
        evidence.operations.floor_revision = 0;
        evidence.operations.root_revision = 0;
        let plan = evaluate(
            MarkerState::Dirty,
            BackendKind::Local,
            Some(&evidence),
            false,
            true,
        );
        assert!(!plan.may_start());
        for step in [
            "audit_reconciled",
            "confirmed_writes_readable",
            "floor_and_root",
        ] {
            assert!(
                plan.steps
                    .iter()
                    .any(|candidate| candidate.id == step
                        && candidate.outcome == StepOutcome::Failed),
                "missing failed step {step}"
            );
        }
        Ok(())
    }

    #[test]
    fn missing_old_authority_term_refuses() -> Result<()> {
        let mut evidence = evidence(BackendKind::Local, Readiness::Announced);
        evidence.previous_authority = None;
        let plan = evaluate(
            MarkerState::Dirty,
            BackendKind::Local,
            Some(&evidence),
            false,
            true,
        );
        assert!(!plan.may_start());
        assert!(plan.refusal_message().contains("old_authority_term"));
        Ok(())
    }

    #[test]
    fn evidence_version_is_checked() -> Result<()> {
        let mut evidence = evidence(BackendKind::Local, Readiness::Announced);
        evidence.version = 2;
        let plan = evaluate(
            MarkerState::Dirty,
            BackendKind::Local,
            Some(&evidence),
            false,
            true,
        );
        assert!(!plan.may_start());
        assert!(plan.refusal_message().contains("evidence_version"));
        Ok(())
    }

    #[test]
    fn clean_marker_starts_normally_without_evidence() -> Result<()> {
        let plan = evaluate(MarkerState::Clean, BackendKind::Local, None, false, true);
        assert_eq!(plan.path(), RecoveryPath::Clean);
        assert!(plan.may_start());
        assert_eq!(plan.admission(), RecoveryAdmission::Normal);
        Ok(())
    }

    #[test]
    fn dirty_marker_without_evidence_fails_closed() -> Result<()> {
        let plan = evaluate(MarkerState::Dirty, BackendKind::Local, None, false, true);
        assert!(!plan.may_start());
        assert!(matches!(
            plan.decision(),
            RecoveryDecision::OperatorRequired { .. }
        ));
        assert!(plan.refusal_message().contains("recovery_evidence"));
        Ok(())
    }

    #[test]
    fn legacy_reconciled_remains_an_explicit_operator_path() -> Result<()> {
        let plan = evaluate(MarkerState::Dirty, BackendKind::Local, None, true, true);
        assert_eq!(plan.path(), RecoveryPath::LegacyReconciled);
        assert!(plan.may_start());
        assert_eq!(plan.admission(), RecoveryAdmission::Normal);
        Ok(())
    }

    #[test]
    fn pre_ready_crash_is_owned_by_the_launcher_not_the_authority() -> Result<()> {
        let mut evidence = evidence(BackendKind::Local, Readiness::NotAnnounced);
        evidence.preserved_snapshot = Some(PreservedSnapshot {
            intent_sha256: digest('1'),
            terminal_sha256: digest('2'),
            state_sha256: digest('3'),
            state_floor_sha256: digest('4'),
            journal_sha256: digest('5'),
        });
        let plan = evaluate(
            MarkerState::Dirty,
            BackendKind::Local,
            Some(&evidence),
            false,
            true,
        );
        assert_eq!(plan.path(), RecoveryPath::InterruptedStartup);
        assert!(!plan.may_start());
        assert!(matches!(
            plan.decision(),
            RecoveryDecision::LauncherRollbackRequired { .. }
        ));
        assert!(plan
            .operator_actions
            .iter()
            .any(|action| action.contains("--recover")));
        Ok(())
    }

    #[test]
    fn unknown_readiness_is_not_treated_as_pre_ready() -> Result<()> {
        let evidence = evidence(BackendKind::Local, Readiness::Unknown);
        let plan = evaluate(
            MarkerState::Dirty,
            BackendKind::Local,
            Some(&evidence),
            false,
            true,
        );
        assert!(!plan.may_start());
        assert_eq!(plan.path(), RecoveryPath::Unclassified);
        assert!(plan.refusal_message().contains("readiness"));
        Ok(())
    }

    #[test]
    fn post_ready_local_recovery_is_restricted_and_keeps_unknown_pending() -> Result<()> {
        let evidence = evidence(BackendKind::Local, Readiness::Announced);
        let plan = evaluate(
            MarkerState::Dirty,
            BackendKind::Local,
            Some(&evidence),
            false,
            true,
        );
        assert_eq!(plan.path(), RecoveryPath::PostReadyCrash);
        assert!(plan.may_start());
        match plan.admission() {
            RecoveryAdmission::Restricted { pending_unknown } => {
                assert_eq!(pending_unknown, vec!["op-unknown".to_owned()]);
            }
            RecoveryAdmission::Normal => panic!("post-ready crash must start restricted"),
        }
        assert_eq!(plan.replay_policy, ReplayPolicy::Never);
        let json = plan.to_json_pretty()?;
        assert!(json.contains("\"replay_policy\": \"never\""));
        assert!(json.contains("op-unknown"));
        Ok(())
    }

    #[test]
    fn s3_post_ready_accepts_only_matching_remote_readback() -> Result<()> {
        let evidence = evidence(BackendKind::S3, Readiness::Announced);
        let plan = evaluate(
            MarkerState::Dirty,
            BackendKind::S3,
            Some(&evidence),
            false,
            true,
        );
        assert!(plan.may_start());
        assert!(plan.admission().is_restricted());
        let mut mismatched = evidence.clone();
        mismatched
            .remote_terminal
            .as_mut()
            .expect("remote")
            .readback_etag = "later-etag".into();
        let plan = evaluate(
            MarkerState::Dirty,
            BackendKind::S3,
            Some(&mismatched),
            false,
            true,
        );
        assert!(!plan.may_start());
        assert!(plan.refusal_message().contains("backend_terminal"));
        Ok(())
    }

    #[test]
    fn remote_backend_rejects_local_process_proof_alone() -> Result<()> {
        let mut evidence = evidence(BackendKind::S3, Readiness::Announced);
        evidence.remote_terminal = None;
        let plan = evaluate(
            MarkerState::Dirty,
            BackendKind::S3,
            Some(&evidence),
            false,
            true,
        );
        assert!(!plan.may_start());
        assert!(plan.refusal_message().contains("remote terminal"));
        Ok(())
    }

    #[test]
    fn evidence_backend_must_match_the_running_backend() -> Result<()> {
        let evidence = evidence(BackendKind::Local, Readiness::Announced);
        let plan = evaluate(
            MarkerState::Dirty,
            BackendKind::S3,
            Some(&evidence),
            false,
            true,
        );
        assert!(!plan.may_start());
        assert!(plan.refusal_message().contains("backend_identity"));
        Ok(())
    }

    #[test]
    fn malformed_operation_identity_blocks_recovery() -> Result<()> {
        let mut evidence = evidence(BackendKind::Local, Readiness::Announced);
        evidence.operations.unknown.push("op-unknown".into());
        let plan = evaluate(
            MarkerState::Dirty,
            BackendKind::Local,
            Some(&evidence),
            false,
            true,
        );
        assert!(!plan.may_start());
        assert!(plan.refusal_message().contains("duplicated"));
        Ok(())
    }

    fn binding(database: &DatabaseId) -> LeaseBinding {
        LeaseBinding {
            database: database.clone(),
            root_incarnation: "root-incarnation".into(),
            authority_term: 1,
            branch_incarnation: 1,
            epoch: 1,
            worker_id: "worker".into(),
            lease_id: "lease".into(),
            grant_id: "grant".into(),
            renewal_sequence: 0,
            role: AccessRole::Writer,
        }
    }

    #[test]
    fn restricted_admission_blocks_session_creation_and_session_work() -> Result<()> {
        let database = DatabaseId::new(
            crate::namespace::Namespace::new("tenant", "database")?,
            "main",
        )?;
        let restricted = RecoveryAdmission::Restricted {
            pending_unknown: vec!["op-unknown".into()],
        };
        assert!(restricted.admits(&AuthorityCommand::GetMetrics));
        assert!(restricted.admits(&AuthorityCommand::GetDatabaseState {
            database: database.clone()
        }));
        assert!(!restricted.admits(&AuthorityCommand::OpenSession {
            binding: binding(&database),
            role: AccessRole::Writer,
        }));
        assert!(!restricted.admits(&AuthorityCommand::Session {
            session: "session-1".into(),
            operation: crate::authority_rpc::protocol::SessionOperation::Validate,
        }));
        assert!(
            RecoveryAdmission::Normal.admits(&AuthorityCommand::OpenSession {
                binding: binding(&database),
                role: AccessRole::Writer,
            })
        );
        Ok(())
    }

    #[test]
    fn post_ready_kill_boundaries_map_to_pending_or_known_without_replay() -> Result<()> {
        #[derive(Clone, Copy)]
        enum Boundary {
            IntentPersisted,
            AuthorityCommitted,
            TerminalPersisted,
            AuditPersisted,
            ResponseSent,
        }
        for boundary in [
            Boundary::IntentPersisted,
            Boundary::AuthorityCommitted,
            Boundary::TerminalPersisted,
            Boundary::AuditPersisted,
            Boundary::ResponseSent,
        ] {
            let mut evidence = evidence(BackendKind::Local, Readiness::Announced);
            match boundary {
                // A kill after readiness but before the terminal/audit account
                // is complete must still fail closed.
                Boundary::IntentPersisted => {
                    evidence.operations.terminal_sha256.clear();
                    let plan = evaluate(
                        MarkerState::Dirty,
                        BackendKind::Local,
                        Some(&evidence),
                        false,
                        true,
                    );
                    assert!(!plan.may_start(), "intent boundary must not start");
                }
                // A committed authority write with an unknown client outcome is
                // carried as pending and never replayed.
                Boundary::AuthorityCommitted | Boundary::ResponseSent => {
                    evidence.operations.unknown = vec!["op-unknown".into()];
                    let plan = evaluate(
                        MarkerState::Dirty,
                        BackendKind::Local,
                        Some(&evidence),
                        false,
                        true,
                    );
                    assert!(plan.may_start());
                    assert_eq!(plan.replay_policy, ReplayPolicy::Never);
                    assert_eq!(plan.admission().pending_unknown(), ["op-unknown"]);
                }
                Boundary::TerminalPersisted | Boundary::AuditPersisted => {
                    evidence.operations.unknown.clear();
                    let plan = evaluate(
                        MarkerState::Dirty,
                        BackendKind::Local,
                        Some(&evidence),
                        false,
                        true,
                    );
                    assert!(plan.may_start());
                    assert_eq!(plan.replay_policy, ReplayPolicy::Never);
                }
            }
        }
        Ok(())
    }

    #[test]
    fn ready_crash_with_evidence_starts_restricted_and_fences_the_old_seal() -> Result<()> {
        let directory = prepare_directory();
        dirty_marker(directory.path());
        let store: Arc<dyn ObjectStore> = Arc::new(MemoryObjectStore::default());
        let (mut old_seal, owner, etag) = seed_dirty_seal(&store);
        let mut evidence = evidence(BackendKind::Local, Readiness::Announced);
        evidence.previous_authority = Some(PreviousAuthority { owner, etag });
        let startup = AuthorityStartup::acquire_with_recovery(
            directory.path(),
            crate::authority_rpc::startup::RecoveryRequest {
                backend: BackendKind::Local,
                evidence: Some(evidence),
                confirmed_quiescence: false,
                plan_out: None,
            },
        )?;
        assert!(startup.admission().is_restricted());
        let authority = OwnershipAuthority::new(store.clone(), Arc::new(SystemLeaseClock));
        let auth = AuthConfig::new(
            "a".repeat(64),
            std::collections::BTreeMap::from([("worker".to_owned(), "b".repeat(64))]),
        )?;
        let server = Server::bind("127.0.0.1:0".parse()?, authority, auth, startup)?;
        assert!(server.recovery_admission().is_restricted());
        // A late old-term publication must lose the conditional write.
        assert!(old_seal.mark_dirty().is_err());
        let observed = super::super::seal::Seal::observe(store.as_ref())?.expect("new seal");
        assert_ne!(observed.owner, "old-owner");
        assert!(!observed.clean);
        drop(server);
        Ok(())
    }

    #[test]
    fn ready_crash_without_old_process_evidence_refuses_and_keeps_marker_dirty() -> Result<()> {
        let directory = prepare_directory();
        dirty_marker(directory.path());
        let store: Arc<dyn ObjectStore> = Arc::new(MemoryObjectStore::default());
        let (_, owner, etag) = seed_dirty_seal(&store);
        let mut evidence = evidence(BackendKind::Local, Readiness::Announced);
        evidence.previous_authority = Some(PreviousAuthority { owner, etag });
        evidence.local_process = None;
        let plan_path = directory.path().join("recovery-plan.json");
        let result = AuthorityStartup::acquire_with_recovery(
            directory.path(),
            crate::authority_rpc::startup::RecoveryRequest {
                backend: BackendKind::Local,
                evidence: Some(evidence),
                confirmed_quiescence: false,
                plan_out: Some(plan_path.clone()),
            },
        );
        assert!(result.is_err());
        assert_eq!(
            std::fs::read(directory.path().join("authority.lock"))?,
            b"dirty\n"
        );
        let plan: serde_json::Value = serde_json::from_slice(&std::fs::read(plan_path)?)?;
        assert_eq!(plan["decision"]["decision"], "operator_required");
        let steps = plan["steps"].as_array().expect("steps");
        assert!(steps
            .iter()
            .any(|step| { step["id"] == "old_process_stopped" && step["outcome"] == "failed" }));
        Ok(())
    }

    #[test]
    fn plan_artifact_records_seal_verification() -> Result<()> {
        let directory = prepare_directory();
        dirty_marker(directory.path());
        let store: Arc<dyn ObjectStore> = Arc::new(MemoryObjectStore::default());
        let (_, owner, etag) = seed_dirty_seal(&store);
        let mut evidence = evidence(BackendKind::Local, Readiness::Announced);
        evidence.previous_authority = Some(PreviousAuthority { owner, etag });
        let plan_path = directory.path().join("recovery-plan.json");
        let startup = AuthorityStartup::acquire_with_recovery(
            directory.path(),
            crate::authority_rpc::startup::RecoveryRequest {
                backend: BackendKind::Local,
                evidence: Some(evidence),
                confirmed_quiescence: false,
                plan_out: Some(plan_path.clone()),
            },
        )?;
        let before: serde_json::Value = serde_json::from_slice(&std::fs::read(&plan_path)?)?;
        assert_eq!(before["decision"]["decision"], "start_restricted");
        assert_eq!(before["replay_policy"], "never");
        assert_eq!(before["seal_verified"], serde_json::Value::Null);
        let authority = OwnershipAuthority::new(store, Arc::new(SystemLeaseClock));
        let auth = AuthConfig::new(
            "a".repeat(64),
            std::collections::BTreeMap::from([("worker".to_owned(), "b".repeat(64))]),
        )?;
        let server = Server::bind("127.0.0.1:0".parse()?, authority, auth, startup)?;
        server.write_recovery_plan()?;
        let after: serde_json::Value = serde_json::from_slice(&std::fs::read(&plan_path)?)?;
        assert_eq!(after["seal_verified"], true);
        assert_eq!(after["decision"]["decision"], "start_restricted");
        Ok(())
    }

    #[test]
    fn restricted_server_refuses_session_work_over_the_rpc() -> Result<()> {
        let directory = prepare_directory();
        dirty_marker(directory.path());
        let store: Arc<dyn ObjectStore> = Arc::new(MemoryObjectStore::default());
        let (_, owner, etag) = seed_dirty_seal(&store);
        let mut evidence = evidence(BackendKind::Local, Readiness::Announced);
        evidence.previous_authority = Some(PreviousAuthority { owner, etag });
        let startup = AuthorityStartup::acquire_with_recovery(
            directory.path(),
            crate::authority_rpc::startup::RecoveryRequest {
                backend: BackendKind::Local,
                evidence: Some(evidence),
                confirmed_quiescence: false,
                plan_out: None,
            },
        )?;
        let store_for_check = store.clone();
        let authority = OwnershipAuthority::new(store, Arc::new(SystemLeaseClock));
        let control_token = "a".repeat(64);
        let auth = AuthConfig::new(
            control_token.clone(),
            std::collections::BTreeMap::from([("worker".to_owned(), "b".repeat(64))]),
        )?;
        let server = Server::bind("127.0.0.1:0".parse()?, authority, auth, startup)?;
        let endpoint = format!("http://{}/", server.local_addr());
        let handle = std::thread::spawn(move || server.run());
        let control = crate::authority_rpc::client::AuthorityClient::new(&endpoint, control_token)?;
        let metrics: serde_json::Value = control.call(AuthorityCommand::GetMetrics)?;
        assert_eq!(metrics["recovery"]["mode"], "restricted");
        let database = DatabaseId::new(
            crate::namespace::Namespace::new("recovery", "database")?,
            "main",
        )?;
        let error = control
            .call(AuthorityCommand::OpenSession {
                binding: binding(&database),
                role: AccessRole::Writer,
            })
            .unwrap_err();
        assert!(error.to_string().contains("authority operation rejected"));
        control.call(AuthorityCommand::Shutdown)?;
        handle.join().expect("authority server panicked")?;
        assert_eq!(
            std::fs::read(directory.path().join("authority.lock"))?,
            b"clean\n"
        );
        let seal = super::super::seal::Seal::observe(store_for_check.as_ref())?.expect("seal");
        assert!(seal.clean);
        let restart = AuthorityStartup::acquire(directory.path(), false)?;
        assert!(!restart.admission().is_restricted());
        let auth = AuthConfig::new(
            "a".repeat(64),
            std::collections::BTreeMap::from([("worker".to_owned(), "b".repeat(64))]),
        )?;
        let server = Server::bind(
            "127.0.0.1:0".parse()?,
            OwnershipAuthority::new(store_for_check, Arc::new(SystemLeaseClock)),
            auth,
            restart,
        )?;
        assert!(!server.recovery_admission().is_restricted());
        Ok(())
    }

    #[test]
    fn evidence_schema_is_strict() -> Result<()> {
        let mut value = serde_json::to_value(evidence(BackendKind::Local, Readiness::Announced))?;
        value["unexpected"] = serde_json::json!(true);
        assert!(serde_json::from_value::<RecoveryEvidence>(value).is_err());
        Ok(())
    }
}
