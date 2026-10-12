package platform

import (
	"context"
	"time"

	"github.com/gorilla/websocket"
)

func (handler *hranaHTTP) closeWebSockets(ctx context.Context) error {
	handler.mu.Lock()
	connections := make([]*websocket.Conn, 0, len(handler.wsConns))
	for connection := range handler.wsConns {
		connections = append(connections, connection)
	}
	handler.mu.Unlock()
	for _, connection := range connections {
		_ = connection.Close()
	}
	for {
		handler.mu.Lock()
		active := handler.wsOpen
		handler.mu.Unlock()
		if active == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}
