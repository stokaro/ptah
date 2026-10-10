package builtin_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/goschema"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/spanner/spannerschema"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/builtintest"
)

// policySource is one table keyed on id, with a timestamp column created_at
// and an integer column expires, whose table directive carries attributes.
func policySource(attributes string) *schemamodel.Database {
	database := must.Must(goschema.ParseSource(builtintest.Annotations(), "events.go", `package entities

//ptah:schema:table name="events" `+attributes+`
type Event struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64
	//ptah:schema:field name="created_at" type="TIMESTAMP"
	CreatedAt string
	//ptah:schema:field name="expires" type="BIGINT UNSIGNED"
	Expires uint64
}
`))
	return &database
}

// TestRowDeletionPolicy_PlatformPropertiesFollowTheTarget pins the source
// spelling: each target's properties render on that target in its own
// spelling, and are scoped away from every other target, as every platform
// property is. Nothing translates a Spanner interval into a YDB one.
func TestRowDeletionPolicy_PlatformPropertiesFollowTheTarget(t *testing.T) {
	database := policySource(`platform.spanner.row_deletion_column="created_at" platform.spanner.row_deletion_interval="30 days" ` +
		`platform.ydb.row_deletion_column="expires" platform.ydb.row_deletion_interval="PT1H" platform.ydb.row_deletion_unit="seconds"`)
	tests := []struct {
		dialect string
		caps    capability.Capabilities
		want    string
	}{
		{dialect: platform.Spanner, caps: capability.SpannerPostgres(), want: `) TTL INTERVAL '30 days' ON "created_at";`},
		{dialect: platform.YDB, caps: capability.YDB262(), want: "WITH (TTL = Interval(\"PT1H\") ON `expires` AS SECONDS)"},
		{dialect: platform.Postgres, caps: capability.Postgres18(), want: ""},
		{dialect: platform.CockroachDB, caps: capability.CockroachDB26(), want: ""},
		{dialect: platform.MySQL, caps: capability.MySQL84(), want: ""},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(database, test.dialect, test.caps)
			c.Assert(err, qt.IsNil)
			rendered := strings.Join(statements, "\n")
			c.Assert(strings.Contains(rendered, "TTL"), qt.Equals, test.want != "")
			c.Assert(rendered, qt.Contains, test.want)
		})
	}
}

// TestRowDeletionPolicy_AnUnboundPolicyIsRefusedElsewhere pins the other half:
// a policy built in Go without a target binding claims every target, and a
// target without an owner for it refuses rather than drops it.
func TestRowDeletionPolicy_AnUnboundPolicyIsRefusedElsewhere(t *testing.T) {
	tests := []struct {
		name    string
		value   schemaext.Value
		dialect string
		wantErr string
	}{
		{name: "Spanner policy on PostgreSQL", dialect: platform.Postgres, wantErr: `(?s).*ptah.run/spanner/row-deletion-policy.*`,
			value: &spannerschema.DesiredRowDeletion{Policy: spannerschema.Policy{Column: "created_at", Interval: "30 days"}}},
		{name: "Spanner policy on YDB", dialect: platform.YDB, wantErr: `(?s).*ptah.run/spanner/row-deletion-policy.*`,
			value: &spannerschema.DesiredRowDeletion{Policy: spannerschema.Policy{Column: "created_at", Interval: "30 days"}}},
		{name: "YDB TTL on Spanner", dialect: platform.Spanner, wantErr: `(?s).*ptah.run/ydb/ttl.*`,
			value: &ydbschema.DesiredTTL{Policy: ydbschema.TTL{Column: "created_at", Interval: "P30D"}}},
		{name: "YDB TTL on MySQL", dialect: platform.MySQL, wantErr: `(?s).*ptah.run/ydb/ttl.*`,
			value: &ydbschema.DesiredTTL{Policy: ydbschema.TTL{Column: "created_at", Interval: "P30D"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "Event", Name: "events", Facets: must.Must(schemaext.NewFacets(test.value))}},
				Fields: []schemamodel.Field{
					{StructName: "Event", Name: "id", Type: "BIGINT", Primary: true},
					{StructName: "Event", Name: "created_at", Type: "TIMESTAMP", Nullable: true},
				},
			}
			statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(database, test.dialect, capability.ForDialect(test.dialect))
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// TestRowDeletionPolicy_ATargetWithoutTheCapabilityRefuses pins the owners'
// capability refusals: a policy on a target without row_deletion_policy, and a
// YDB TTL on an integer column on one without row_deletion_policy_epoch_column.
// Rendered without the setting, the server would keep every row the
// declaration said to delete.
func TestRowDeletionPolicy_ATargetWithoutTheCapabilityRefuses(t *testing.T) {
	tests := []struct {
		name       string
		attributes string
		dialect    string
		caps       capability.Capabilities
		wantErr    string
	}{
		{
			name: "Spanner", dialect: platform.Spanner, caps: capability.SpannerPostgres().With(capability.RowDeletionPolicy, false),
			attributes: `platform.spanner.row_deletion_column="created_at" platform.spanner.row_deletion_interval="30 days"`,
			wantErr:    `(?s)spanner: table "events" declares a row deletion policy, which requires target capability row_deletion_policy; .*`,
		},
		{
			name: "YDB", dialect: platform.YDB, caps: capability.YDB262().With(capability.RowDeletionPolicyEpochColumn, false).With(capability.RowDeletionPolicy, false),
			attributes: `platform.ydb.row_deletion_column="created_at" platform.ydb.row_deletion_interval="P30D"`,
			wantErr:    `the TTL of table "events", which requires target capability row_deletion_policy, unavailable on this ydb target`,
		},
		{
			name: "YDB on an integer column", dialect: platform.YDB, caps: capability.YDB251().With(capability.RowDeletionPolicyEpochColumn, false),
			attributes: `platform.ydb.row_deletion_column="expires" platform.ydb.row_deletion_interval="PT1H" platform.ydb.row_deletion_unit="SECONDS"`,
			wantErr: `the TTL of table "events" reads an integer column counting SECONDS, which requires target capability ` +
				`row_deletion_policy_epoch_column, unavailable on this ydb target`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(policySource(test.attributes), test.dialect, test.caps)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// TestRowDeletionPolicy_AMisspelledPropertyIsRefused pins that each owner
// claims every row_deletion-prefixed key of its platform group: a misspelled
// or miscased property, and a unit Spanner's clause has no spelling for, is
// refused by name rather than left as a table option nothing reads.
func TestRowDeletionPolicy_AMisspelledPropertyIsRefused(t *testing.T) {
	tests := []struct {
		name       string
		attributes string
		dialect    string
		wantErr    string
	}{
		{
			name: "a misspelled name", dialect: platform.YDB,
			attributes: `platform.ydb.row_deletion_column="created_at" platform.ydb.row_deletion_intervall="P30D"`,
			wantErr:    `(?s).*unknown row deletion property "row_deletion_intervall": the policy takes row_deletion_column, row_deletion_interval, row_deletion_unit.*`,
		},
		{
			name: "an upper-case name", dialect: platform.Spanner,
			attributes: `platform.spanner.ROW_DELETION_COLUMN="created_at" platform.spanner.row_deletion_interval="30 days"`,
			wantErr:    `(?s).*unknown row deletion property "ROW_DELETION_COLUMN": property names are lower case, as "row_deletion_column".*`,
		},
		{
			name: "a unit on Spanner", dialect: platform.Spanner,
			attributes: `platform.spanner.row_deletion_column="expires" platform.spanner.row_deletion_interval="30 days" platform.spanner.row_deletion_unit="SECONDS"`,
			wantErr:    `(?s).*unknown row deletion property "row_deletion_unit": the policy takes row_deletion_column, row_deletion_interval.*`,
		},
		{
			name: "a YDB interval on Spanner", dialect: platform.Spanner,
			attributes: `platform.spanner.row_deletion_column="created_at" platform.spanner.row_deletion_interval="P30D"`,
			wantErr:    `(?s).*interval "P30D" is not a number of months, weeks, days or hours, such as 30 days.*`,
		},
		{
			name: "a Spanner interval on YDB", dialect: platform.YDB,
			attributes: `platform.ydb.row_deletion_column="created_at" platform.ydb.row_deletion_interval="30 days"`,
			wantErr:    `(?s).*interval "30 days" is not an ISO 8601 duration YDB takes.*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(policySource(test.attributes), test.dialect, capability.ForDialect(test.dialect))
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}
