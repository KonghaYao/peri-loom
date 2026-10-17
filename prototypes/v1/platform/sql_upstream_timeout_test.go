package platform

import (
	"net/http"
	"testing"
	"time"
)

func TestSQLUpstreamTimeoutBounds(test *testing.T) {
	tokens := map[string]string{"worker-a": sqlTestToken(test)}
	for _, duration := range []time.Duration{0, 29 * time.Second, 301 * time.Second} {
		if _, err := NewSQLUpstreamWithTimeout(tokens, duration); err == nil {
			test.Fatalf("accepted unsupported timeout %s", duration)
		}
	}
	for _, duration := range []time.Duration{30 * time.Second, 150 * time.Second} {
		created, err := NewSQLUpstreamWithTimeout(tokens, duration)
		if err != nil {
			test.Fatal(err)
		}
		upstream := created.(*sqlHTTPUpstream)
		transport := upstream.client.Transport.(*http.Transport)
		if upstream.timeout != duration || upstream.client.Timeout != duration || transport.ResponseHeaderTimeout != duration {
			test.Fatalf("SQL timeouts disagree for %s", duration)
		}
	}
}

// The worker reclaims a parked socket after its own keep-alive window
// (KEEP_ALIVE_IDLE, 5 s in prototypes/v1/src/worker_rpc/transport.rs). This
// client must retire a socket first: otherwise a request can start on one the
// worker has already decided to close, and the reply is reported ambiguous.
func TestSQLUpstreamRetiresIdleSocketsBeforeWorkerReclamation(test *testing.T) {
	created, err := NewSQLUpstream(map[string]string{"worker-a": sqlTestToken(test)})
	if err != nil {
		test.Fatal(err)
	}
	transport := created.(*sqlHTTPUpstream).client.Transport.(*http.Transport)
	if transport.IdleConnTimeout <= 0 || transport.IdleConnTimeout >= 5*time.Second {
		test.Fatalf("client idle timeout %s must stay below the worker's 5 s reclamation", transport.IdleConnTimeout)
	}
}
