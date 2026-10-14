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
	knownMoved := false
	fmt.Fprintln(output, "Use :execute SQL or :query SQL. Transactions stay on this session. :reconnect discards the old session without replay.")
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if line == ":quit" {
			if knownMoved {
				return nil
			}
			return closeSession(session)
		}
		if line == ":reconnect" {
			if !knownMoved {
				if err := closeSession(session); err != nil {
					return err
				}
			}
			next, err := open()
			if err != nil {
				return err
			}
			session = next
			knownMoved = false
			fmt.Fprintln(output, "New session opened; no SQL or transaction replay.")
			continue
		}
		action, sql, found := strings.Cut(line, " ")
		if !found || (action != ":execute" && action != ":query") {
			fmt.Fprintln(output, "Explicit :execute or :query required.")
			continue
		}
		if knownMoved {
			fmt.Fprintln(output, "Session moved; use :reconnect explicitly. No SQL or transaction replay.")
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		if action == ":execute" {
			err = session.Execute(ctx, sql, "")
			if err == nil {
				if err := json.NewEncoder(output).Encode(map[string]bool{"executed": true}); err != nil {
					cancel()
					return err
				}
			}
		} else {
			var rows [][]any
			rows, err = session.Query(ctx, sql, "")
			if err == nil {
				if err := json.NewEncoder(output).Encode(rows); err != nil {
					cancel()
					return err
				}
			}
		}
		cancel()
		if err != nil {
			if !knownSessionMoved(err) {
				return err
			}
			knownMoved = true
			fmt.Fprintln(output, err)
			fmt.Fprintln(output, "Operation not replayed. Use :reconnect explicitly.")
		}
	}
	if scanner.Err() != nil {
		return errors.New("shell input limit or read failure")
	}
	if knownMoved {
		return nil
	}
	return closeSession(session)
}

func knownSessionMoved(err error) bool {
	var failure *sdk.Error
	return errors.As(err, &failure) && failure != nil && failure.Code == "session_moved" && !failure.OutcomeUnknown
}
