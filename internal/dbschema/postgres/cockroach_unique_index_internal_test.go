package postgres

// White-box testing required: cockroachUniqueIndexOnly reads the text
// pg_get_constraintdef returns for one constraint row, and which UNIQUE rows
// the reader leaves out is not observable through the exported reader API
// without a live CockroachDB server.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
)

// TestCockroachUniqueIndexOnly reads the definitions CockroachDB v26.3.2
// prints for unique indexes. A key with a direction is an index only a
// CREATE UNIQUE INDEX can describe; every other row stays a constraint.
func TestCockroachUniqueIndexOnly(t *testing.T) {
	tests := []struct {
		name           string
		dialect        string
		constraintType string
		definition     string
		want           bool
	}{
		{name: "a descending key", dialect: platform.CockroachDB, constraintType: "UNIQUE", definition: "UNIQUE (a DESC)", want: true},
		{name: "a descending second key", dialect: platform.CockroachDB, constraintType: "UNIQUE", definition: "UNIQUE (a ASC, b DESC)", want: true},
		{name: "ascending keys", dialect: platform.CockroachDB, constraintType: "UNIQUE", definition: "UNIQUE (a ASC, b ASC)", want: false},
		{name: "a column named desc", dialect: platform.CockroachDB, constraintType: "UNIQUE", definition: `UNIQUE ("desc" ASC)`, want: false},
		{name: "a payload after the key", dialect: platform.CockroachDB, constraintType: "UNIQUE", definition: "UNIQUE (a ASC) STORING (b)", want: false},
		{name: "a primary key", dialect: platform.CockroachDB, constraintType: "PRIMARY KEY", definition: "PRIMARY KEY (id DESC)", want: false},
		{name: "PostgreSQL", dialect: platform.Postgres, constraintType: "UNIQUE", definition: "UNIQUE (a DESC)", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := cockroachUniqueIndexOnly(test.dialect, test.constraintType, test.definition)

			c.Assert(got, qt.Equals, test.want)
		})
	}
}
