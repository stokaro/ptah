package provider_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
)

// queryDialect is a YQL-like declaration, as a provider would publish it.
func queryDialect() *protocol.QueryDialect {
	return &protocol.QueryDialect{
		Placeholder: "dollar-named",
		Quoting:     &protocol.QueryQuoting{Style: "yql", Bare: "never", Paths: true, Keywords: []string{"select", "from"}},
		Query: &protocol.QuerySpelling{ChecksCapabilities: true, UnsignedBounds: true, Upsert: "upsert-into",
			UpsertRefusal: "the engine has no ON CONFLICT", UnsupportedJoins: []string{"full"}},
		DefaultCapabilities: map[string]bool{"returning_clause": true, "offset_without_limit": false},
	}
}

func TestQueryDialectDescription_HappyPath(t *testing.T) {
	c := qt.New(t)
	description := dataDescription()
	description.Engines[0].QueryDialect = queryDialect()
	c.Assert(provider.ValidateDescription(description, nil), qt.IsNil)
	c.Assert(provider.ValidateQueryDialect(nil), qt.IsNil)
	c.Assert(provider.ValidateQueryDialect(&protocol.QueryDialect{Placeholder: "question"}), qt.IsNil)
}

func TestQueryDialectDescription_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.QueryDialect)
	}{
		{"no placeholder", func(d *protocol.QueryDialect) { d.Placeholder = "" }},
		{"placeholder with a space", func(d *protocol.QueryDialect) { d.Placeholder = "dollar named" }},
		{"quote style of two lines", func(d *protocol.QueryDialect) { d.Quoting.Style = "yql\n" }},
		{"bare names with a space", func(d *protocol.QueryDialect) { d.Quoting.Bare = "where recorded" }},
		{"upsert with a space", func(d *protocol.QueryDialect) { d.Query.Upsert = "upsert into" }},
		{"empty keyword", func(d *protocol.QueryDialect) { d.Quoting.Keywords = []string{""} }},
		{"repeated keyword", func(d *protocol.QueryDialect) { d.Quoting.Keywords = []string{"select", "select"} }},
		{"join with a space", func(d *protocol.QueryDialect) { d.Query.UnsupportedJoins = []string{"full outer"} }},
		{"refusal of two lines", func(d *protocol.QueryDialect) { d.Query.UpdateRefusal = "no\nupdate" }},
		{"refusal too long", func(d *protocol.QueryDialect) { d.Query.UpsertRefusal = strings.Repeat("a", 513) }},
		{"order term with a control character", func(d *protocol.QueryDialect) { d.Query.FetchOrderBy = "(SELECT\x00NULL)" }},
		{"capability name with a space at its end", func(d *protocol.QueryDialect) { d.DefaultCapabilities["returning_clause "] = true }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			description := dataDescription()
			description.Engines[0].QueryDialect = queryDialect()
			test.mutate(description.Engines[0].QueryDialect)
			c.Assert(provider.ValidateDescription(description, nil), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}
