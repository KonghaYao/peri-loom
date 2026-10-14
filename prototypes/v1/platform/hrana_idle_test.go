package platform

import (
	"testing"
	"time"
)

func TestHranaCleanupUsesSQLRequestBudget(t *testing.T) {
	for _, testCase := range []struct {
		requestTimeout time.Duration
		want           time.Duration
	}{
		{requestTimeout: 0, want: 20 * time.Second},
		{requestTimeout: 10 * time.Second, want: 20 * time.Second},
		{requestTimeout: 30 * time.Second, want: 30 * time.Second},
		{requestTimeout: 180 * time.Second, want: 180 * time.Second},
	} {
		if got := hranaCleanupTimeout(testCase.requestTimeout); got != testCase.want {
			t.Errorf("request timeout %v: cleanup timeout = %v, want %v", testCase.requestTimeout, got, testCase.want)
		}
	}
}
