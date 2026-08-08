package goschema_test

import (
	"encoding/json"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/goschema"
)

// TestDatabaseJSONEncodingIsTheFingerprint pins the one property that lets a
// new field be added to [goschema.Database] at all: the JSON encoding of this
// struct IS the desired-state fingerprint that plan files record and that
// `schema plan --name-format` renders through `.ToHash`. A field that
// serialized unconditionally would change that fingerprint for every schema
// anyone has already planned against, which is exactly what
// stokaro/ptah#1276's "adding coverage changes no existing plan" promises does
// not happen.
//
// The first row is the regression: NotDescribed shipped without `omitzero` and
// encoded as `"NotDescribed":{"Kinds":null,"Objects":null}` on every schema,
// live or hand-written.
func TestDatabaseJSONEncodingIsTheFingerprint(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name        string
		database    goschema.Database
		wantPresent bool
	}{
		{
			name:        "a description that declares no limits encodes as it did before the field existed",
			database:    goschema.Database{},
			wantPresent: false,
		},
		{
			name:        "a populated description still encodes no record while it declares no limits",
			database:    goschema.Database{Tables: []goschema.Table{{Name: "users", StructName: "User"}}},
			wantPresent: false,
		},
		{
			name: "a description that declares a limit says so",
			database: goschema.Database{
				NotDescribed: coverage.Set{}.WithKind(coverage.Extension),
			},
			wantPresent: true,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			encoded, err := json.Marshal(&test.database)
			c.Assert(err, qt.IsNil)
			c.Assert(strings.Contains(string(encoded), "NotDescribed"), qt.Equals, test.wantPresent)
		})
	}
}
