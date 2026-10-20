package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
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
		fmt.Fprintln(output, "Commands: create delete snapshot branch clone restore restore-point pitr suspend resume migrate status checkpoint schema compact refs gc shell sql")
		fmt.Fprintln(output, "Isolation: operations diagnostics operations/decision --operation-id ID --decision retain_isolation_new_database_required; operations/retire-local requires launcher-owned stop+Wait proof; isolation/release requires the same stop proof and the authority's fencing attestation, and never clears isolation without them.")
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
