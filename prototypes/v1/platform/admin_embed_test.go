package platform

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAdminHandlerServesBuiltConsole(t *testing.T) {
	handler := AdminHandler()
	for _, path := range []string{"/", "/console.js", "/console.css"} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest("GET", path, nil))
		if response.Code != 200 || response.Body.Len() == 0 {
			t.Fatalf("%s: status=%d size=%d", path, response.Code, response.Body.Len())
		}
		if response.Header().Get("Content-Security-Policy") == "" {
			t.Fatalf("%s: missing content security policy", path)
		}
		if path == "/" {
			for _, asset := range []string{"/console.js", "/console.css"} {
				if !strings.Contains(response.Body.String(), asset) {
					t.Fatalf("index does not reference %s", asset)
				}
			}
		}
	}
}
