package sdk

import (
	"context"
	"errors"
)

type Session struct {
	client     *Client
	identifier string
	readonly   bool
	operations chan struct{}
	closed     bool
	unusable   bool
}

type executeReply struct {
	Executed bool `json:"executed"`
}

func (session Session) String() string   { return "<peri gateway session>" }
func (session Session) GoString() string { return session.String() }
func (session *Session) ReadOnly() bool  { return session.readonly }

func (client *Client) Open(ctx context.Context, database DatabaseID) (*Session, error) {
	if database.Validate() != nil {
		return nil, &Error{Code: "invalid_database"}
	}
	identifier, err := RandomID()
	if err != nil {
		return nil, err
	}
	var opened struct {
		Session  string `json:"session"`
		Readonly bool   `json:"readonly"`
	}
	err = client.call(ctx, "/v1/sql/open", identifier, map[string]any{"database": database, "request_id": identifier}, &opened, true)
	if err != nil {
		return nil, err
	}
	if len(opened.Session) != 32 {
		return nil, &Error{Code: "outcome_unknown", RequestID: identifier, OutcomeUnknown: true}
	}
	for _, character := range opened.Session {
		if !(character >= '0' && character <= '9' || character >= 'a' && character <= 'f') {
			return nil, &Error{Code: "outcome_unknown", RequestID: identifier, OutcomeUnknown: true}
		}
	}
	return &Session{client: client, identifier: opened.Session, readonly: opened.Readonly, operations: make(chan struct{}, 1)}, nil
}

func (session *Session) enter(ctx context.Context) (func(), error) {
	select {
	case session.operations <- struct{}{}:
		return func() { <-session.operations }, nil
	case <-ctx.Done():
		return nil, &Error{Code: "request_cancelled"}
	}
}

func (session *Session) sql(ctx context.Context, action, sql, identifier string, output any) error {
	release, err := session.enter(ctx)
	if err != nil {
		return err
	}
	defer release()
	if identifier == "" {
		identifier, err = RandomID()
		if err != nil {
			return err
		}
	}
	if !label.MatchString(identifier) {
		return &Error{Code: "invalid_request_id"}
	}
	if session.closed || session.unusable {
		return &Error{Code: "session_unusable", RequestID: identifier}
	}
	if sql == "" {
		return &Error{Code: "invalid_sql", RequestID: identifier}
	}
	err = session.client.call(ctx, "/v1/sql/"+action, identifier, map[string]string{"session": session.identifier, "sql": sql, "request_id": identifier}, output, true)
	if err == nil && ((action == "execute" && !output.(*executeReply).Executed) || (action == "query" && *output.(*[][]any) == nil)) {
		err = &Error{Code: "outcome_unknown", RequestID: identifier, OutcomeUnknown: true}
	}
	var failure *Error
	if errors.As(err, &failure) && (failure.OutcomeUnknown || failure.Code == "session_moved") {
		session.unusable = true
	}
	return err
}

func (session *Session) Execute(ctx context.Context, sql, identifier string) error {
	var result executeReply
	if err := session.sql(ctx, "execute", sql, identifier, &result); err != nil {
		return err
	}
	return nil
}

func (session *Session) Query(ctx context.Context, sql, identifier string) ([][]any, error) {
	var rows [][]any
	err := session.sql(ctx, "query", sql, identifier, &rows)
	return rows, err
}

func (session *Session) Close(ctx context.Context) error {
	release, err := session.enter(ctx)
	if err != nil {
		return err
	}
	defer release()
	if session.closed {
		return nil
	}
	identifier, err := RandomID()
	if err != nil {
		return err
	}
	var result struct {
		Closed bool `json:"closed"`
	}
	err = session.client.call(ctx, "/v1/sql/close", identifier, map[string]string{"session": session.identifier, "request_id": identifier}, &result, true)
	if err == nil && !result.Closed {
		err = &Error{Code: "outcome_unknown", RequestID: identifier, OutcomeUnknown: true}
	}
	if err == nil {
		session.closed = true
	} else {
		var failure *Error
		if errors.As(err, &failure) && (failure.OutcomeUnknown || failure.Code == "session_moved") {
			session.unusable = true
		}
	}
	return err
}
