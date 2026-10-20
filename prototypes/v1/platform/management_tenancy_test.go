package platform

import "testing"

func TestDefaultQuotasRaiseRequestRate(t *testing.T) {
	quotas := defaultQuotas(Quotas{})
	if quotas.RequestsPerSecond != 500 {
		t.Fatalf("default requests_per_second = %d, want 500", quotas.RequestsPerSecond)
	}
	if quotas.MaxDatabases != 8 || quotas.MaxSessions != 16 || quotas.MaxConcurrent != 4 ||
		quotas.MaxSQLBytes != 65536 || quotas.MaxResultBytes != 4*1024*1024 {
		t.Fatalf("non-rate defaults changed: %+v", quotas)
	}
	if explicit := defaultQuotas(Quotas{RequestsPerSecond: 42}); explicit.RequestsPerSecond != 42 {
		t.Fatalf("explicit requests_per_second = %d, want 42", explicit.RequestsPerSecond)
	}
}

func TestCreateProjectAppliesDefaultRequestRate(t *testing.T) {
	fixture := newGatewayFixture(t)
	management := &Management{config: ManagementConfig{State: fixture.state}}
	if _, err := management.createProject(Principal{Role: "admin", Project: "*"}, managementRequest{ID: "fresh", Organization: "org", Region: "local"}); err != nil {
		t.Fatal(err)
	}
	state, err := fixture.state.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	project, found := state.Projects["fresh"]
	if !found {
		t.Fatal("created project missing from durable state")
	}
	if project.Quotas.RequestsPerSecond != 500 {
		t.Fatalf("created project requests_per_second = %d, want 500", project.Quotas.RequestsPerSecond)
	}
}
