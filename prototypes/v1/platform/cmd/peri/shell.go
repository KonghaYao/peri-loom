package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	sdk "peri-loom/platform/sdk/go"
)

func shell(client *sdk.Client, database sdk.DatabaseID, input io.Reader, output io.Writer, timeout time.Duration) error {
	open := func() (*sdk.Session, error) {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		return client.Open(ctx, database)
	}
	session, err := open()
	if err != nil {
		return err
	}
	defer func() { closeSession(session) }()
	fmt.Fprintln(output, "Use :execute SQL or :query SQL. Transactions stay on this session. :reconnect discards the old session without replay.")
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if line == ":quit" {
			return nil
		}
		if line == ":reconnect" {
			closeSession(session)
			next, err := open()
			if err != nil {
				return err
			}
			session = next
			fmt.Fprintln(output, "New session opened; no SQL or transaction replay.")
			continue
		}
		action, sql, found := strings.Cut(line, " ")
		if !found || (action != ":execute" && action != ":query") {
			fmt.Fprintln(output, "Explicit :execute or :query required.")
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		if action == ":execute" {
			err = session.Execute(ctx, sql, "")
			if err == nil {
				err = json.NewEncoder(output).Encode(map[string]bool{"executed": true})
			}
		} else {
			var rows [][]any
			rows, err = session.Query(ctx, sql, "")
			if err == nil {
				err = json.NewEncoder(output).Encode(rows)
			}
		}
		cancel()
		if err != nil {
			var failure *sdk.Error
			if !errors.As(err, &failure) {
				return errors.New("shell operation unavailable")
			}
			fmt.Fprintln(output, failure)
			if failure.OutcomeUnknown || failure.Code == "session_moved" {
				fmt.Fprintln(output, "Operation not replayed. Resolve uncertain outcome, then use :reconnect explicitly.")
			}
		}
	}
	if scanner.Err() != nil {
		return errors.New("shell input limit or read failure")
	}
	return nil
}
