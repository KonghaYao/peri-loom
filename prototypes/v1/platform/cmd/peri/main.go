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
		var failure *sdk.Error
		if errors.As(err, &failure) {
			fmt.Fprintln(os.Stderr, failure)
		} else {
			fmt.Fprintln(os.Stderr, "peri: invalid command, configuration, or unavailable service")
		}
		os.Exit(1)
	}
}

func run(arguments []string, input io.Reader, output io.Writer) error {
	if len(arguments) == 0 || arguments[0] == "help" {
		fmt.Fprintln(output, "peri COMMAND --endpoint HTTPS_URL --database TENANT/DATABASE/BRANCH [--credential-file PRIVATE_FILE] [--ca-file CERTIFICATE]")
		fmt.Fprintln(output, "Commands: create delete snapshot branch clone restore restore-point pitr suspend resume migrate status checkpoint schema compact refs gc shell sql")
		fmt.Fprintln(output, "Credentials: PERI_API_KEY environment or private credential file; SQL requires --execute/--query, shell uses :execute/:query/:reconnect/:quit. No SQL/session replay.")
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
	execute := flags.String("execute", "", "")
	query := flags.String("query", "", "")
	timeout := flags.Duration("timeout", 30*time.Second, "")
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
	options, err := tlsOptions(*caFile, *timeout)
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
		defer closeSession(session)
		if *execute != "" {
			if err := session.Execute(ctx, *execute, *identifier); err != nil {
				return err
			}
			return json.NewEncoder(output).Encode(map[string]bool{"executed": true})
		}
		rows, err := session.Query(ctx, *query, *identifier)
		if err != nil {
			return err
		}
		return json.NewEncoder(output).Encode(rows)
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
	request := sdk.ManagementRequest{Database: &database, RequestID: *identifier, Project: *project, Name: *name, Snapshot: *snapshot, WorkerID: *worker, Sequence: *sequence, TimestampMS: *timestamp}
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

func closeSession(session *sdk.Session) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = session.Close(ctx)
}
