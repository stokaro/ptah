package ydbrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/spanner/spannerschema"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbrender"
	"ptah.run/dialect/ydb/ydbschema"
)

func ttlRegistry() renderer.Extensions {
	return must.Must(renderer.NewExtensions(ydbrender.TTLHandler()))
}

// TestTTLHandler_RendersEachTransition pins the statement each transition
// lowers to: SET (TTL = ...) whether the table had a TTL or not, and RESET
// (TTL) to remove one, each applied to local-ydb 26.2.1.14 and 25.1.4.7.
func TestTTLHandler_RendersEachTransition(t *testing.T) {
	stored := &ydbschema.ObservedTTL{Policy: ydbschema.TTL{Column: "ts", Interval: "P30D"}}
	tests := []struct {
		name   string
		table  string
		change ydbdiff.TTL
		want   string
	}{
		{name: "an addition", table: "events", change: ydbdiff.TTL{After: &ydbschema.DesiredTTL{Policy: ydbschema.TTL{Column: "ts", Interval: "P30D"}}},
			want: "ALTER TABLE `events` SET (TTL = Interval(\"P30D\") ON `ts`);"},
		{name: "a change to an integer column", table: "app.events", change: ydbdiff.TTL{Before: stored, After: &ydbschema.DesiredTTL{Policy: ydbschema.TTL{Column: "e", Interval: "PT1H", Unit: "SECONDS"}}},
			want: "ALTER TABLE `app/events` SET (TTL = Interval(\"PT1H\") ON `e` AS SECONDS);"},
		{name: "a removal", table: "events", change: ydbdiff.TTL{Before: stored}, want: "ALTER TABLE `events` RESET (TTL);"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := ttlRegistry().Render(renderer.ExtensionContext{
				Target: "ydb", Capabilities: capability.YDB262(), Parent: &ast.AlterTableNode{Name: test.table},
			}, ast.AlterExtension, &ydbast.AlterTTL{Change: test.change})
			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, []string{test.want})
		})
	}
}

func TestTTLHandler_FailurePath(t *testing.T) {
	epoch := &ydbast.AlterTTL{Change: ydbdiff.TTL{After: &ydbschema.DesiredTTL{Policy: ydbschema.TTL{Column: "e", Interval: "P1D", Unit: "SECONDS"}}}}
	tests := []struct {
		name    string
		ctx     renderer.ExtensionContext
		wantErr string
	}{
		{name: "another target", ctx: renderer.ExtensionContext{Target: "spanner", Capabilities: capability.SpannerPostgres(), Parent: &ast.AlterTableNode{Name: "t"}},
			wantErr: `a YDB TTL cannot be rendered for "spanner"`},
		{name: "a capability set without the key", ctx: renderer.ExtensionContext{Target: "ydb",
			Capabilities: capability.YDB262().With(capability.RowDeletionPolicyEpochColumn, false).With(capability.RowDeletionPolicy, false), Parent: &ast.AlterTableNode{Name: "t"}},
			wantErr: `the TTL of table "t", which requires target capability row_deletion_policy, unavailable on this ydb target`},
		{name: "an integer column without the key", ctx: renderer.ExtensionContext{Target: "ydb",
			Capabilities: capability.YDB262().With(capability.RowDeletionPolicyEpochColumn, false), Parent: &ast.AlterTableNode{Name: "t"}},
			wantErr: `the TTL of table "t" reads an integer column counting SECONDS, which requires target capability row_deletion_policy_epoch_column, unavailable on this ydb target`},
		{name: "no parent", ctx: renderer.ExtensionContext{Target: "ydb", Capabilities: capability.YDB262()},
			wantErr: `.*requires an ALTER TABLE parent.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := ttlRegistry().Render(test.ctx, ast.AlterExtension, epoch)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}

func TestCreateTableTTL_HappyPath(t *testing.T) {
	types := map[string]string{"ts": "Timestamp", "e": "Uint64"}
	tests := []struct {
		name   string
		facets schemaext.Facets
		want   string
	}{
		{name: "a table without a TTL", want: ""},
		{name: "a date column", facets: must.Must(schemaext.NewFacets(&ydbschema.DesiredTTL{Policy: ydbschema.TTL{Column: "ts", Interval: "PT720H"}})),
			want: "TTL = Interval(\"PT720H\") ON `ts`"},
		{name: "an integer column", facets: must.Must(schemaext.NewFacets(&ydbschema.DesiredTTL{Policy: ydbschema.TTL{Column: "e", Interval: "PT1H", Unit: "NANOSECONDS"}})),
			want: "TTL = Interval(\"PT1H\") ON `e` AS NANOSECONDS"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			setting, err := ydbrender.CreateTableTTL("ydb", capability.YDB262(), "t", test.facets, types)
			c.Assert(err, qt.IsNil)
			c.Assert(setting, qt.Equals, test.want)
		})
	}
}

func TestCreateTableTTL_FailurePath(t *testing.T) {
	types := map[string]string{"ts": "Timestamp", "n": "Int64"}
	declared := func(policy ydbschema.TTL) schemaext.Facets {
		return must.Must(schemaext.NewFacets(&ydbschema.DesiredTTL{Policy: policy}))
	}
	tests := []struct {
		name    string
		target  string
		caps    capability.Capabilities
		facets  schemaext.Facets
		wantErr string
	}{
		{name: "a column the table does not declare", target: "ydb", caps: capability.YDB262(), facets: declared(ydbschema.TTL{Column: "gone", Interval: "P1D"}),
			wantErr: `the TTL of table "t": it reads column "gone", which the table does not declare .*`},
		{name: "a signed integer column", target: "ydb", caps: capability.YDB262(), facets: declared(ydbschema.TTL{Column: "n", Interval: "P1D", Unit: "SECONDS"}),
			wantErr: `the TTL of table "t": column "n" is Int64, .*`},
		{name: "an invalid declaration", target: "ydb", caps: capability.YDB262(), facets: declared(ydbschema.TTL{Column: "ts", Interval: "30 days"}),
			wantErr: `the TTL of table "t": desired model "ptah.run/ydb/ttl": invalid feature value: interval "30 days" .*`},
		{name: "a capability set without the key", target: "ydb", caps: capability.YDB262().With(capability.RowDeletionPolicyEpochColumn, false).With(capability.RowDeletionPolicy, false),
			facets: declared(ydbschema.TTL{Column: "ts", Interval: "P1D"}), wantErr: `the TTL of table "t", which requires target capability row_deletion_policy, .*`},
		{name: "another target", target: "spanner", caps: capability.SpannerPostgres(), facets: declared(ydbschema.TTL{Column: "ts", Interval: "P1D"}),
			wantErr: `a YDB TTL cannot be rendered for "spanner"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			setting, err := ydbrender.CreateTableTTL(test.target, test.caps, "t", test.facets, types)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(setting, qt.Equals, "")
		})
	}
}

// TestValidateTableFacets_RefusesWhatNoYDBOwnerRenders pins that a CREATE
// TABLE carrying another owner's facet, or an observation, is refused rather
// than written without it.
func TestValidateTableFacets_RefusesWhatNoYDBOwnerRenders(t *testing.T) {
	tests := []struct {
		name    string
		facets  schemaext.Facets
		wantErr error
	}{
		{name: "another owner's facet", facets: must.Must(schemaext.NewFacets(&spannerschema.DesiredRowDeletion{Policy: spannerschema.Policy{Column: "ts", Interval: "30 days"}})),
			wantErr: ptaherr.ErrUnsupportedFeature},
		{name: "an observation", facets: must.Must(schemaext.NewFacets(&ydbschema.ObservedTTL{Policy: ydbschema.TTL{Column: "ts", Interval: "P1D"}})),
			wantErr: schemaext.ErrInvalidValue},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbrender.ValidateTableFacets(test.facets), qt.ErrorIs, test.wantErr)
		})
	}
}
