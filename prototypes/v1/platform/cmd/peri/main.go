package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"time"

	sdk "peri-loom/platform/sdk/go"
)

func main() {
	if err := run(os.Args[1:], os.Stdin, os.Stdout); err != nil {
		reportFailure(os.Stderr, err)
		os.Exit(1)
	}
}

func reportFailure(writer io.Writer, err error) {
	var failure *sdk.Error
	if errors.As(err, &failure) {
		// Print the returned error, not only its typed cause: a failed session
		// cleanup is appended as context while the original failure stays wrapped.
		fmt.Fprintln(writer, err)
		return
	}
	fmt.Fprintln(writer, "peri: invalid command, configuration, or unavailable service")
}

func run(arguments []string, input io.Reader, output io.Writer) error {
	if len(arguments) == 0 || arguments[0] == "help" {
		fmt.Fprintln(output, "peri COMMAND --endpoint HTTPS_URL --database TENANT/DATABASE/BRANCH [--credential-file PRIVATE_FILE] [--ca-file CERTIFICATE] [--connection-reuse]")
		fmt.Fprintln(output, "Commands: create delete snapshot branch clone restore restore-point pitr suspend resume migrate status checkpoint schema compact refs gc shell sql recover")
		fmt.Fprintln(output, "Isolation: operations diagnostics operations/decision --operation-id ID --decision retain_isolation_new_database_required; operations/retire-local requires launcher-owned stop+Wait proof; isolation/release requires the same stop proof and the authority's fencing attestation, and never clears isolation without them.")
		fmt.Fprintln(output, "Recovery: recover runs operations/decision, operations/retire-local and isolation/release in that order with deterministic request ids. Run it only against the SQL-disabled instance started by dev.py --recover, which requires the owned worker stop proofs and the authority's fencing attestation; --request-id is not accepted. Release does not delete data or prove the unknown operation's contents, and it never clears the ownership marker.")
		fmt.Fprintln(output, "Operation history retains at most 256 safe known terminal records; unresolved/isolation records are never evicted. Missing history proves neither non-execution nor safe replay; request IDs remain correlation-only.")
		fmt.Fprintln(output, "Credentials: PERI_API_KEY environment or private credential file; SQL requires --execute/--query, shell uses :execute/:query/:reconnect/:quit. No SQL/session replay.")
		fmt.Fprintln(output, "Transport: --connection-reuse (default off) reuses keep-alive connections. A request that fails before its body is written is not ambiguous; one that wrote the request and lost the reply ends as outcome_unknown. Requests are never retried.")
		return nil
	}
	command := arguments[0]
	flags := flag.NewFlagSet("peri", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	endpoint := flags.String("endpoint", os.Getenv("PERI_ENDPOINT"), "")
	credentialFile := flags.String("credential-file", os.Getenv("PERI_CREDENTIAL_FILE"), "")
	caFile := flags.String("ca-file", os.Getenv("PERI_CA_FILE"), "")
	databaseName := flags.String("database", "", "")
	project := flags.String("project", "", "")
	role := flags.String("role", "writer", "")
	name := flags.String("name", "", "")
	snapshot := flags.String("snapshot", "", "")
	worker := flags.String("worker-id", "", "")
	sequence := flags.Uint64("sequence", 0, "")
	timestamp := flags.Uint64("timestamp-ms", 0, "")
	safety := flags.Uint64("safety-window-ms", 60000, "")
	collect := flags.Bool("collect", false, "")
	identifier := flags.String("request-id", "", "")
	operationID := flags.String("operation-id", "", "")
	decision := flags.String("decision", "", "")
	execute := flags.String("execute", "", "")
	query := flags.String("query", "", "")
	timeout := flags.Duration("timeout", 30*time.Second, "")
	connectionReuse := flags.Bool("connection-reuse", false, "")
	if flags.Parse(arguments[1:]) != nil || flags.NArg() != 0 || *timeout <= 0 {
		return errors.New("invalid arguments")
	}
	if command == "recover" && *identifier != "" {
		return errors.New("recover derives deterministic request ids; --request-id is not accepted")
	}
	parts := strings.Split(*databaseName, "/")
	if len(parts) != 3 {
		return errors.New("database identity required")
	}
	database := sdk.DatabaseID{Namespace: sdk.Namespace{Tenant: parts[0], Database: parts[1]}, Branch: parts[2]}
	if err := database.Validate(); err != nil {
		return err
	}
	credential, err := readCredential(*credentialFile)
	if err != nil {
		return err
	}
	options, err := tlsOptions(*caFile, *timeout, *connectionReuse)
	if err != nil {
		return err
	}
	client, err := sdk.NewClient(*endpoint, credential, options)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	if command == "recover" {
		return recoverIsolation(ctx, client, database, output)
	}
	if command == "shell" {
		return shell(client, database, input, output, *timeout)
	}
	if command == "sql" {
		if (*execute == "") == (*query == "") {
			return errors.New("choose execute or query explicitly")
		}
		session, err := client.Open(ctx, database)
		if err != nil {
			return err
		}
		if *execute != "" {
			if err := session.Execute(ctx, *execute, *identifier); err != nil {
				return cleanupAfterFailure(session, err)
			}
			if err := json.NewEncoder(output).Encode(map[string]bool{"executed": true}); err != nil {
				return cleanupAfterFailure(session, err)
			}
			return closeSession(session)
		}
		rows, err := session.Query(ctx, *query, *identifier)
		if err != nil {
			return cleanupAfterFailure(session, err)
		}
		if err := json.NewEncoder(output).Encode(rows); err != nil {
			return cleanupAfterFailure(session, err)
		}
		return closeSession(session)
	}
	if *execute != "" || *query != "" {
		return errors.New("SQL flags require sql command")
	}
	if command == "gc" && !*collect {
		command = "gc-inspect"
	}
	if *timestamp != 0 && *sequence != 0 {
		return errors.New("choose sequence or timestamp")
	}
	request := sdk.ManagementRequest{Database: &database, RequestID: *identifier, Project: *project, Name: *name, Snapshot: *snapshot, WorkerID: *worker, Sequence: *sequence, TimestampMS: *timestamp, OperationID: *operationID, Decision: *decision}
	if command == "create" || command == "branch" || command == "clone" {
		request.Role = *role
	}
	if command == "gc" {
		request.SafetyWindowMS = *safety
	}
	result, err := client.Management(ctx, command, request)
	if err != nil {
		return err
	}
	return json.NewEncoder(output).Encode(result)
}

// recoveryOperation is the slice of a durable operation record the one-step
// recovery needs to choose its next request.
type recoveryOperation struct {
	OperationID           string `json:"operation_id"`
	State                 string `json:"state"`
	Decision              string `json:"decision"`
	LocalResourcesRetired bool   `json:"local_resources_retired"`
	ReleaseRequestID      string `json:"release_request_id"`
}

type recoveryOperations struct {
	Operations []recoveryOperation `json:"operations"`
}

func (record recoveryOperation) pendingRelease() bool {
	return record.State == "quarantined" && record.ReleaseRequestID == ""
}

var recoveryRequestIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// recoveryRequestID derives a deterministic id from the operation the step
// acts on, so rerunning after a failure repeats the same durable request
// instead of inventing a second decision.
func recoveryRequestID(prefix, operationID string) (string, error) {
	identifier := prefix + operationID
	if !recoveryRequestIDPattern.MatchString(identifier) {
		return "", errors.New("operation id cannot form a recovery request id")
	}
	return identifier, nil
}

func firstPendingOperation(records []recoveryOperation, match func(recoveryOperation) bool) *recoveryOperation {
	for index := range records {
		if match(records[index]) {
			return &records[index]
		}
	}
	return nil
}

func readRecoveryOperations(ctx context.Context, client *sdk.Client, database sdk.DatabaseID) (recoveryOperations, json.RawMessage, error) {
	result, err := client.Management(ctx, "operations", sdk.ManagementRequest{Database: &database})
	if err != nil {
		return recoveryOperations{}, nil, err
	}
	var state recoveryOperations
	if err := json.Unmarshal(result, &state); err != nil {
		return recoveryOperations{}, nil, errors.New("invalid operations response")
	}
	return state, result, nil
}

func writeRecoveryOperations(output io.Writer, result json.RawMessage) error {
	return json.NewEncoder(output).Encode(result)
}

// recoverIsolation is the one-step form of the documented three-step release.
// It stops at the first failed step: the next step needs the previous durable
// record, so continuing would act without the proof the server checks. Every
// step re-reads and prints the operation view, exactly like the manual flow.
func recoverIsolation(ctx context.Context, client *sdk.Client, database sdk.DatabaseID, output io.Writer) error {
	state, result, err := readRecoveryOperations(ctx, client, database)
	if err != nil {
		return err
	}
	pending := firstPendingOperation(state.Operations, func(record recoveryOperation) bool { return record.pendingRelease() })
	if pending == nil {
		return writeRecoveryOperations(output, result)
	}
	for _, record := range state.Operations {
		if !record.pendingRelease() || record.Decision != "" {
			continue
		}
		identifier, err := recoveryRequestID("decision-", record.OperationID)
		if err != nil {
			return err
		}
		if _, err := client.Management(ctx, "operations/decision", sdk.ManagementRequest{Database: &database, RequestID: identifier, OperationID: record.OperationID, Decision: "retain_isolation_new_database_required"}); err != nil {
			return err
		}
		if state, result, err = readRecoveryOperations(ctx, client, database); err != nil {
			return err
		}
		if err := writeRecoveryOperations(output, result); err != nil {
			return err
		}
	}
	if retire := firstPendingOperation(state.Operations, func(record recoveryOperation) bool {
		return record.pendingRelease() && !record.LocalResourcesRetired
	}); retire != nil {
		identifier, err := recoveryRequestID("retire-", retire.OperationID)
		if err != nil {
			return err
		}
		if _, err := client.Management(ctx, "operations/retire-local", sdk.ManagementRequest{Database: &database, RequestID: identifier}); err != nil {
			return err
		}
		if state, result, err = readRecoveryOperations(ctx, client, database); err != nil {
			return err
		}
		if err := writeRecoveryOperations(output, result); err != nil {
			return err
		}
	}
	if release := firstPendingOperation(state.Operations, func(record recoveryOperation) bool { return record.pendingRelease() }); release != nil {
		identifier, err := recoveryRequestID("release-", release.OperationID)
		if err != nil {
			return err
		}
		if _, err := client.Management(ctx, "isolation/release", sdk.ManagementRequest{Database: &database, RequestID: identifier}); err != nil {
			return err
		}
		if _, result, err = readRecoveryOperations(ctx, client, database); err != nil {
			return err
		}
		return writeRecoveryOperations(output, result)
	}
	return nil
}

// cleanupAfterFailure releases a session whose outcome is settled. An unknown
// outcome or a moved session stays untouched: reconnecting is explicit, and a
// cleanup request must never be mistaken for evidence that resolves the SQL.
// The original failure remains the command result; a failed close is appended
// as context instead of replacing it.
func cleanupAfterFailure(session *sdk.Session, failure error) error {
	var typed *sdk.Error
	if errors.As(failure, &typed) && typed != nil && (typed.OutcomeUnknown || typed.Code == "session_moved") {
		return failure
	}
	if closeErr := closeSession(session); closeErr != nil {
		return fmt.Errorf("%w (session cleanup failed: %v)", failure, closeErr)
	}
	return failure
}

func closeSession(session *sdk.Session) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return session.Close(ctx)
}
