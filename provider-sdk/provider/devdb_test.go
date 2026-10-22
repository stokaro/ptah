package provider_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
)

func devDescription() *protocol.Description {
	description, _, _ := creationExchange()
	description.Features = append(description.Features, provider.DevDatabaseFeature)
	engine := description.Engines[0]
	engine.Features = append(engine.Features, provider.DevDatabaseFeature)
	return description
}

func devEngineID() string { return devDescription().Engines[0].Id }

func devRequest() *protocol.ProvisionDevRequest {
	return &protocol.ProvisionDevRequest{Engine: devEngineID(), Spec: "docker://ydb/24.3"}
}

func devLease() *protocol.DevDatabase {
	return &protocol.DevDatabase{Lease: "lease-1", Url: "grpc://localhost:2136/local"}
}

func devRefusal(kind protocol.DevDatabaseRefusal_Kind, reason string) *protocol.DevDatabase {
	return &protocol.DevDatabase{Refusal: &protocol.DevDatabaseRefusal{Kind: kind, Reason: reason}}
}

func TestDevDatabaseDescription_HappyPath(t *testing.T) {
	c := qt.New(t)
	c.Assert(provider.ValidateDescription(devDescription(), nil), qt.IsNil)
}

func TestDevDatabaseDescription_FailurePath(t *testing.T) {
	c := qt.New(t)
	description := devDescription()
	description.Features = description.Features[:len(description.Features)-1]
	c.Assert(provider.ValidateDescription(description, nil), qt.ErrorIs, provider.ErrProtocol)
}

func TestProvisionDevRequest_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		spec string
	}{
		{"operator spec", "docker://ydb/24.3"},
		{"default spec", ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := devRequest()
			request.Spec = test.spec
			c.Assert(provider.ValidateProvisionDevRequest(devDescription(), request), qt.IsNil)
		})
	}
}

func TestProvisionDevRequest_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.Description, *protocol.ProvisionDevRequest)
	}{
		{"unknown engine", func(_ *protocol.Description, r *protocol.ProvisionDevRequest) { r.Engine = "other" }},
		{"no service", func(d *protocol.Description, _ *protocol.ProvisionDevRequest) { d.Features = nil }},
		{"engine without the service", func(d *protocol.Description, _ *protocol.ProvisionDevRequest) {
			d.Engines[0].Features = d.Engines[0].Features[:len(d.Engines[0].Features)-1]
		}},
		{"control character", func(_ *protocol.Description, r *protocol.ProvisionDevRequest) { r.Spec = "docker://a\nb" }},
		{"invalid UTF-8", func(_ *protocol.Description, r *protocol.ProvisionDevRequest) { r.Spec = "docker://\xff" }},
		{"too long", func(_ *protocol.Description, r *protocol.ProvisionDevRequest) { r.Spec = strings.Repeat("a", 1025) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			description, request := devDescription(), devRequest()
			test.mutate(description, request)
			c.Assert(provider.ValidateProvisionDevRequest(description, request), qt.ErrorIs, provider.ErrProtocol)
		})
	}
	c := qt.New(t)
	c.Assert(provider.ValidateProvisionDevRequest(devDescription(), nil), qt.ErrorIs, provider.ErrProtocol)
}

func TestDevDatabase_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		result *protocol.DevDatabase
	}{
		{"lease", devLease()},
		{"unsupported", devRefusal(protocol.DevDatabaseRefusal_UNSUPPORTED, "no image for release 1.0")},
		{"unavailable", devRefusal(protocol.DevDatabaseRefusal_UNAVAILABLE, "container engine is not running")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(provider.ValidateDevDatabase(devDescription(), devRequest(), test.result), qt.IsNil)
		})
	}
}

func TestDevDatabase_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		result *protocol.DevDatabase
	}{
		{"missing reply", nil},
		{"empty reply", &protocol.DevDatabase{}},
		{"no address", &protocol.DevDatabase{Lease: "lease-1"}},
		{"no lease", &protocol.DevDatabase{Url: "grpc://localhost:2136/local"}},
		{"no scheme", &protocol.DevDatabase{Lease: "lease-1", Url: "//localhost:2136/local"}},
		{"credentials in the address", &protocol.DevDatabase{Lease: "lease-1", Url: "grpc://user:secret@localhost:2136/local"}}, // #nosec G101 -- fixture address, not a credential
		{"refusal with a lease", &protocol.DevDatabase{Lease: "lease-1", Refusal: devRefusal(protocol.DevDatabaseRefusal_UNSUPPORTED, "no").Refusal}},
		{"refusal with an address", &protocol.DevDatabase{Url: "grpc://localhost", Refusal: devRefusal(protocol.DevDatabaseRefusal_UNSUPPORTED, "no").Refusal}},
		{"refusal without a kind", devRefusal(protocol.DevDatabaseRefusal_KIND_UNSPECIFIED, "no")},
		{"refusal without a reason", devRefusal(protocol.DevDatabaseRefusal_UNSUPPORTED, " ")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(provider.ValidateDevDatabase(devDescription(), devRequest(), test.result), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestReleaseDevRequest_HappyPath(t *testing.T) {
	c := qt.New(t)
	request := &protocol.ReleaseDevRequest{Engine: devEngineID(), Lease: "lease-1"}
	c.Assert(provider.ValidateReleaseDevRequest(devDescription(), request), qt.IsNil)
	c.Assert(provider.ValidateReleasedDev(devDescription(), request, &protocol.Empty{}), qt.IsNil)
}

func TestReleaseDevRequest_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		request *protocol.ReleaseDevRequest
	}{
		{"missing", nil},
		{"no lease", &protocol.ReleaseDevRequest{Engine: devEngineID()}},
		{"unknown engine", &protocol.ReleaseDevRequest{Engine: "other", Lease: "lease-1"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(provider.ValidateReleaseDevRequest(devDescription(), test.request), qt.ErrorIs, provider.ErrProtocol)
		})
	}
	c := qt.New(t)
	request := &protocol.ReleaseDevRequest{Engine: devEngineID(), Lease: "lease-1"}
	c.Assert(provider.ValidateReleasedDev(devDescription(), request, nil), qt.ErrorIs, provider.ErrProtocol)
}
