package platform

import (
	"testing"
	"time"
)

func TestLocalLeaseTTLStaysWithinAuthorityLimit(test *testing.T) {
	if got := localLeaseTTL(30 * time.Second); got != 30*time.Second {
		test.Fatalf("default lease changed: %s", got)
	}
	if got := localLeaseTTL(180 * time.Second); got != 60*time.Second {
		test.Fatalf("local import lease must reach the authority maximum: %s", got)
	}
}
