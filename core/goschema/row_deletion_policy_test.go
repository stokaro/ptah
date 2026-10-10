package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
)

// TestParse_RowDeletionPolicyIsAPlatformProperty pins the Go annotation
// spelling: the Spanner policy and the YDB TTL are platform.spanner and
// platform.ydb properties their owners decode, each in its own target's
// spelling. The source claims complete knowledge of the YDB TTL, so a table
// without the properties requests none; the Spanner claim belongs to the
// Spanner owner, which spannersource tests.
func TestParse_RowDeletionPolicyIsAPlatformProperty(t *testing.T) {
	c := qt.New(t)

	database, err := goschema.ParseSource(noOwners, "events.go", `package entities

//ptah:schema:table name="events" platform.spanner.row_deletion_column="created_at" platform.spanner.row_deletion_interval="30 days" platform.ydb.row_deletion_column="expires" platform.ydb.row_deletion_interval="PT1H" platform.ydb.row_deletion_unit="seconds"
type Event struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64
}

//ptah:schema:table name="plain"
type Plain struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64
}
`)

	c.Assert(err, qt.IsNil)
	c.Assert(database.Tables[0].Overrides, qt.DeepEquals, map[string]map[string]string{
		"spanner": {"row_deletion_column": "created_at", "row_deletion_interval": "30 days"},
		"ydb":     {"row_deletion_column": "expires", "row_deletion_interval": "PT1H", "row_deletion_unit": "seconds"},
	})
	c.Assert(database.Tables[0].Facets.IsZero(), qt.IsTrue)
}

// TestParse_RowDeletionPolicyHasNoBareAttribute pins that the policy's
// property names are not attributes of the table directive: written without a
// platform prefix, which would not say whose spelling the interval is in, one
// is an unknown attribute.
func TestParse_RowDeletionPolicyHasNoBareAttribute(t *testing.T) {
	c := qt.New(t)

	_, err := goschema.ParseSource(noOwners, "events.go", `package entities

//ptah:schema:table name="events" row_deletion_column="created_at" row_deletion_interval="P30D"
type Event struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64
}
`)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnknownAttribute)
	c.Assert(err, qt.ErrorMatches, `(?s).*row_deletion_(column|interval).*`)
}
