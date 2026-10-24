package provider_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
)

// The operator's address and the realm reply carry credentials on purpose:
// they are fixtures, not secrets.
const (
	realmAddress = "grpcs://admin:secret@ydb.example:2135/local"              // #nosec G101 -- fixture address, not a credential
	realmURL     = "grpcs://admin:secret@ydb.example:2135/local?dev_realm=r1" // #nosec G101 -- fixture address, not a credential
)

func realmRequest() *protocol.ProvisionDevRequest {
	return &protocol.ProvisionDevRequest{Engine: devEngineID(), Address: realmAddress}
}

func TestProvisionDevRealm_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		url  string
	}{
		{"the operator's credentials", realmURL},
		{"no credentials", "grpcs://ydb.example:2135/local?dev_realm=r1"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result := &protocol.DevDatabase{Lease: "realm-1", Url: test.url}
			c.Assert(provider.ValidateDevDatabase(devCleanupDescription(), realmRequest(), result), qt.IsNil)
		})
	}
	c := qt.New(t)
	request := &protocol.ProvisionDevRequest{Engine: devEngineID(), Spec: "docker+ydb://_/ydbplatform/local-ydb:24.3/local"}
	c.Assert(provider.ValidateProvisionDevRequest(devCleanupDescription(), request), qt.IsNil)
}

func TestProvisionDevRealmRequest_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.Description, *protocol.ProvisionDevRequest)
	}{
		{"an engine without realms", func(d *protocol.Description, _ *protocol.ProvisionDevRequest) {
			d.Engines[0].Features = []string{provider.DevDatabaseFeature}
		}},
		{"a spec beside the address", func(_ *protocol.Description, r *protocol.ProvisionDevRequest) { r.Spec = "docker://ydb/24.3" }},
		{"an address without a host", func(_ *protocol.Description, r *protocol.ProvisionDevRequest) { r.Address = "grpcs:///local" }},
		{"an address without a scheme", func(_ *protocol.Description, r *protocol.ProvisionDevRequest) { r.Address = "//ydb.example:2135/local" }},
		{"an address with a control character", func(_ *protocol.Description, r *protocol.ProvisionDevRequest) { r.Address = "grpcs://ydb.example/\x01" }},
		{"an address too long", func(_ *protocol.Description, r *protocol.ProvisionDevRequest) {
			r.Address = "grpcs://ydb.example/" + strings.Repeat("a", 1024)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			description, request := devCleanupDescription(), realmRequest()
			test.mutate(description, request)
			c.Assert(provider.ValidateProvisionDevRequest(description, request), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

// A realm reply cannot send the operator's credentials to another server, or
// open the database as someone else.
func TestProvisionDevRealmReply_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		url  string
	}{
		{"another host", "grpcs://admin:secret@other.example:2135/local?dev_realm=r1"},  // #nosec G101 -- fixture address, not a credential
		{"another port", "grpcs://admin:secret@ydb.example:2136/local?dev_realm=r1"},    // #nosec G101 -- fixture address, not a credential
		{"another scheme", "grpc://admin:secret@ydb.example:2135/local?dev_realm=r1"},   // #nosec G101 -- fixture address, not a credential
		{"another identity", "grpcs://root:secret@ydb.example:2135/local?dev_realm=r1"}, // #nosec G101 -- fixture address, not a credential
		{"no scheme", "//ydb.example:2135/local?dev_realm=r1"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result := &protocol.DevDatabase{Lease: "realm-1", Url: test.url}
			c.Assert(provider.ValidateDevDatabase(devCleanupDescription(), realmRequest(), result), qt.ErrorIs, provider.ErrProtocol)
		})
	}
	c := qt.New(t)
	// A provisioned database's address never carries credentials, whatever
	// the engine declares.
	provisioned := &protocol.ProvisionDevRequest{Engine: devEngineID(), Spec: "docker://ydb/24.3"}
	c.Assert(provider.ValidateDevDatabase(devCleanupDescription(), provisioned, &protocol.DevDatabase{Lease: "l", Url: realmURL}), qt.ErrorIs, provider.ErrProtocol)
}
