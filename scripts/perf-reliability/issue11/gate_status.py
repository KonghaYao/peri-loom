"""Issue11 gate_status；按证据职责隔离。"""
from issue11.gate_common import PASS, PHASE_PASS, INCOMPLETE, NOT_RUN, FAIL

def _stability_problems(runs, acceptance):
    problems = []
    grouped = {}
    for run in runs:
        if not run.get("scenario"):
            continue
        if run.get("arm") != "B":
            continue
        grouped.setdefault((run["scenario"], run.get("state")), []).append(run)
    enforced = set(acceptance["round_stability"].get("gate_enforced_for", []))
    maximum = acceptance["round_stability"].get("max_ratio", 1.25)
    for (scenario_id, _), members in grouped.items():
        if scenario_id not in enforced or len(members) < 2:
            continue
        values = [member["stats"][acceptance["round_stability"]["metric"]] for member in members]
        if min(values) <= 0:
            continue
        ratio = max(values) / min(values)
        if ratio > maximum:
            problems.append(("FAIL", "round_instability: " + scenario_id
                             + " ratio=%.3f" % ratio))
    return problems


def _combine(sections, problems, profile, diagnostic):
    hard = [reason for severity, reason in problems if severity == FAIL]
    incomplete = [reason for severity, reason in problems
                  if severity in {INCOMPLETE, NOT_RUN}]
    if any(section["status"] == FAIL for section in sections.values()):
        return FAIL
    if hard:
        return FAIL
    if incomplete:
        return INCOMPLETE
    if profile == "quick" or diagnostic:
        return PHASE_PASS
    return PASS


def _section_problems(sections):
    reasons = []
    for name, section in sections.items():
        if section["status"] != PASS:
            reasons.extend(name + ": " + reason for reason in section["reasons"])
    return reasons


def _result(status, reasons, sections, profile, formal_pass, runs):
    unique = []
    for reason in reasons:
        if reason not in unique:
            unique.append(reason)
    return {
        "status": status,
        "exit_code": 0 if status == PASS else 1,
        "formal_pass": bool(formal_pass),
        "phase_only": status == PHASE_PASS,
        "profile": profile,
        "reasons": unique,
        "sections": sections,
        "runs": runs,
        "summary_used": False,
    }
