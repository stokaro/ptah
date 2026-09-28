package goschematodb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/migration/schemadiff"
)

// TestToDBSchema_PrimaryKeyMethodStaysIdempotent compares a MariaDB document
// whose primary key asks for HASH with itself. Without the method on the
// catalog side, the key reads as BTREE, and `schema diff` of the document with
// itself plans dropping and adding the key (stokaro/ptah#3853).
func TestToDBSchema_PrimaryKeyMethodStaysIdempotent(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t", PrimaryKey: []string{"id"}, PrimaryKeyMethod: "HASH"}},
		Fields: []schemamodel.Field{{StructName: "T", Name: "id", Type: "INT", Primary: true}},
	}

	diff := schemadiff.CompareWithDialect(db, goschematodb.ToDBSchema(db, platform.MariaDB), platform.MariaDB)

	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("%#v", diff))
}
