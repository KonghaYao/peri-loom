package platform

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"

	"peri-loom/platform/sqlcontract"
)

// contractSources are the Go sources of every decider of the failure contract:
// the platform itself, its SDK, and the two harnesses that consume it through a
// module replace. The scan below walks all of them.
var contractSources = []string{".", "./sdk/go", "../stress", "../stress/faultprobe"}

// failureContractRegistry lists the non-test files that name both published
// evidence fields, with the reason each one may. The authority's own fields are
// unexported, which stops a consumer from reading them, but Go cannot stop a
// consumer from declaring its own: this registry is the check that catches one,
// and a new entry means a new decoder of the failure contract.
var failureContractRegistry = map[string]string{
	"platform/sqlcontract/contract.go":         "the authority that decodes them",
	"platform/sqlcontract/vectors.go":          "the authority's conformance kit",
	"platform/gateway_http.go":                 "the only writer of the client reply",
	"platform/management_operations.go":        "the management prompt that publishes an unknown outcome",
	"platform/operation_state.go":              "the durable operation record",
	"stress/client.go":                         "the harness's own artifact fields",
	"stress/faultprobe/client.go":              "the probe's own artifact fields",
	"platform/sdk/typescript/client.ts":        "the published TypeScript client's copy of the rule",
	"platform/sdk/typescript/dist/client.js":   "the built TypeScript client",
	"platform/sdk/typescript/dist/client.d.ts": "the built TypeScript client",
	"platform/frontend/src/api.ts":             "the console, which reads the shared browser authority",
	"platform/web/sqlcontract.js":              "the browser clients' authority",
	"platform/web/sqlcontract.d.ts":            "the browser authority's types",
	"platform/web/rpc.js":                      "the legacy browser client, which also validates the reply shape",
	"platform/web/console.js":                  "the built console",
	"platform/web/app.js":                      "the legacy console shell, which renders the fields",
}

// TestHranaNeverNamesTheFailureContract pins the adapter family that regressed
// three times. Its members decide from sqlcontract alone, so neither published
// evidence field may appear in them at all: the scan fails the moment one of
// them grows a predicate of its own again.
func TestHranaNeverNamesTheFailureContract(t *testing.T) {
	for _, name := range contractFileNames(t, ".") {
		base := filepath.Base(name)
		if !strings.HasPrefix(base, "hrana_") || strings.HasSuffix(base, "_test.go") {
			continue
		}
		body, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		for _, field := range []string{"execution_completed", "may_have_committed", "ExecutionCompleted", "MayHaveCommitted"} {
			if strings.Contains(string(body), field) {
				t.Errorf("%s names %s: the Hrana adapter must decide through sqlcontract", name, field)
			}
		}
	}
}

// TestNoUnregisteredConsumerDecodesTheFailureContract fails on any non-test file
// outside the registry that names both published evidence fields, which is what
// declaring a private decoder of the contract looks like.
func TestNoUnregisteredConsumerDecodesTheFailureContract(t *testing.T) {
	for _, module := range contractSources {
		for _, name := range contractFileNames(t, module) {
			base := filepath.Base(name)
			if strings.HasSuffix(base, "_test.go") || strings.HasSuffix(name, ".mjs") {
				continue
			}
			if !contractSource(filepath.Base(name)) {
				continue
			}
			body, err := os.ReadFile(name)
			if err != nil {
				t.Fatal(err)
			}
			text := string(body)
			_ = text
			if !strings.Contains(text, "execution_completed") || !strings.Contains(text, "may_have_committed") {
				continue
			}
			if _, registered := failureContractRegistry[contractKey(t, name)]; registered {
				continue
			}
			t.Errorf("%s decodes the failure contract without being registered in failureContractRegistry", name)
		}
	}
}

// TestRegistryEntriesStillExist keeps the registry honest: an entry that no
// longer matches a file would hide the next unregistered decoder behind a stale
// list.
func TestRegistryEntriesStillExist(t *testing.T) {
	found := map[string]bool{}
	for _, module := range append(append([]string{}, contractSources...), "./web", "./frontend/src") {
		for _, name := range contractFileNames(t, module) {
			found[contractKey(t, name)] = true
		}
	}
	names := make([]string, 0, len(failureContractRegistry))
	for name := range failureContractRegistry {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if !found[name] {
			t.Errorf("registered entry %q no longer matches a file", name)
		}
	}
}

// contractSource reports whether a file can hold a decoder of the contract.
func contractSource(name string) bool {
	for _, suffix := range []string{".go", ".ts", ".js", ".mjs"} {
		if strings.HasSuffix(name, suffix) && !strings.HasSuffix(name, "_test.go") {
			return true
		}
	}
	return false
}

// contractKey names a file the way failureContractRegistry does: relative to
// prototypes/v1, which is the parent of this package.
func contractKey(t *testing.T, path string) string {
	t.Helper()
	absolute, err := filepath.Abs(path)
	if err != nil {
		t.Fatal(err)
	}
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	relative, err := filepath.Rel(root, absolute)
	if err != nil {
		t.Fatal(err)
	}
	return filepath.ToSlash(relative)
}

// contractTree is the source listing the scans work from. It is built once per
// test binary and skips the directories that cannot hold a decoder, so the guard
// stays cheap next to the suites it protects.
var contractTree struct {
	once  sync.Once
	names []string
	err   error
}

func contractFileNames(t *testing.T, _ string) []string {
	t.Helper()
	contractTree.once.Do(func() {
		for _, root := range append(append([]string{}, contractSources...), "./web", "./frontend/src") {
			contractTree.err = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
				if err != nil {
					return err
				}
				if info.IsDir() {
					if skippedContractDirectory[info.Name()] {
						return filepath.SkipDir
					}
					return nil
				}
				contractTree.names = append(contractTree.names, path)
				return nil
			})
			if contractTree.err != nil {
				return
			}
		}
	})
	if contractTree.err != nil {
		t.Fatal(contractTree.err)
	}
	return contractTree.names
}

var skippedContractDirectory = map[string]bool{
	"node_modules": true, ".git": true, "target": true, "vendor": true, "__pycache__": true,
}

// TestBrowserContractVocabularyMatchesTheGoContract reads the shared browser
// module and asserts its vocabulary equals the Go one, so a code added to the
// platform fails here until the browser clients are extended too.
func TestBrowserContractVocabularyMatchesTheGoContract(t *testing.T) {
	body, err := os.ReadFile(filepath.Join("web", "sqlcontract.js"))
	if err != nil {
		t.Fatal(err)
	}
	entry := regexp.MustCompile(`(?m)^\s{2}([a-z][a-z0-9_]*): "([a-z]+)",$`)
	browser := map[string]string{}
	for _, match := range entry.FindAllStringSubmatch(string(body), -1) {
		browser[match[1]] = match[2]
	}
	if len(browser) == 0 {
		t.Fatal("the browser contract module has no vocabulary")
	}
	for _, code := range sqlcontract.Codes() {
		var failure sqlcontract.Failure
		if err := failureFromCode(code, &failure); err != nil {
			t.Fatal(err)
		}
		if browser[code] != classLabel(failure.Class()) {
			t.Errorf("browser contract classifies %s as %q, the Go contract as %q", code, browser[code], classLabel(failure.Class()))
		}
	}
	for code := range browser {
		known := false
		for _, published := range sqlcontract.Codes() {
			if published == code {
				known = true
				break
			}
		}
		if !known {
			t.Errorf("browser contract publishes %s, which the Go contract does not", code)
		}
	}
}

func failureFromCode(code string, failure *sqlcontract.Failure) error {
	return json.Unmarshal([]byte(`{"code":"`+code+`"}`), failure)
}

func classLabel(class sqlcontract.Class) string {
	switch class {
	case sqlcontract.Statement:
		return "statement"
	case sqlcontract.Uncertain:
		return "uncertain"
	case sqlcontract.Request:
		return "request"
	case sqlcontract.Limit:
		return "limit"
	case sqlcontract.Access:
		return "access"
	case sqlcontract.Session:
		return "session"
	case sqlcontract.Capacity:
		return "capacity"
	case sqlcontract.Placement:
		return "placement"
	case sqlcontract.Platform:
		return "platform"
	case sqlcontract.Unsupported:
		return "unsupported"
	}
	return "unrecognized"
}
