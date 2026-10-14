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
