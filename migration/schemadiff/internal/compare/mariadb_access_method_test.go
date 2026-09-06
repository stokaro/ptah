package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
	"ptah.run/migration/schemadiff/internal/compare"
)

// Only one of the two engines records the USING clause it was given, so only
// one can be compared on it. Measured 2026-09-03, `KEY k USING HASH (a)` read
// back from information_schema.STATISTICS.INDEX_TYPE:
//
//	            MySQL 8.4.11   MariaDB 11.8.9
//	InnoDB      BTREE          HASH
//	MEMORY      HASH           HASH
//	MyISAM      BTREE          HASH
//	Aria        --             HASH
//	ARCHIVE     error 1030     error 1286
//
// MariaDB records the declaration on every engine that takes an index, so a
// desired HASH against a server reporting BTREE is a real difference there.
// MySQL records it only on MEMORY, so the same comparison on the default
// engine would plan a rebuild the server immediately undoes. See
// stokaro/ptah#2834.

func accessMethodPair(desiredType, reportedType string) (*schemamodel.Database, *catalog.Database) {
	return &schemamodel.Database{
		Indexes: []schemamodel.Index{
			{Name: "kh", TableName: "t", Fields: []string{"a"}, Type: desiredType},
		},
	}, &catalog.Database{
		Indexes: []catalog.Index{
			{Name: "kh", TableName: "t", Columns: []string{"a"}, Method: reportedType},
		},
	}
}

func TestIndexes_MariaDBAccessMethod_FailurePath(t *testing.T) {
	tests := []struct {
		name         string
		desiredType  string
		reportedType string
	}{
		{name: "hash asked, btree reported", desiredType: "HASH", reportedType: "BTREE"},
		{name: "hash asked, nothing reported", desiredType: "HASH", reportedType: ""},
		{name: "lower-case spelling", desiredType: "hash", reportedType: "BTREE"},
	}

	want := []difftypes.IndexRef{{Name: "kh", TableName: "t"}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired, current := accessMethodPair(test.desiredType, test.reportedType)
			diff := &difftypes.SchemaDiff{}

			compare.IndexesWithDialect(desired, current, diff, platform.MariaDB)

			c.Assert(diff.IndexAdditions(), qt.DeepEquals, want)
			c.Assert(diff.IndexRemovals(), qt.DeepEquals, want)
		})
	}
}

// The quiet rows, and each is a control against a different wrong fix: a
// matching method converges, an index asking for nothing takes whatever the
// engine chose, and MySQL does not react at all because its catalog records
// the clause only on MEMORY.
func TestIndexes_MariaDBAccessMethod_HappyPath(t *testing.T) {
	tests := []struct {
		name         string
		dialect      string
		desiredType  string
		reportedType string
	}{
		{name: "mariadb hash matches hash", dialect: platform.MariaDB, desiredType: "HASH", reportedType: "HASH"},
		{name: "mariadb hash matches lower case", dialect: platform.MariaDB, desiredType: "HASH", reportedType: "hash"},
		{name: "mariadb asks for nothing", dialect: platform.MariaDB, desiredType: "", reportedType: "HASH"},
		{name: "mariadb btree is the default", dialect: platform.MariaDB, desiredType: "BTREE", reportedType: ""},
		{name: "mysql hash against btree", dialect: platform.MySQL, desiredType: "HASH", reportedType: "BTREE"},
		{name: "mysql hash against nothing", dialect: platform.MySQL, desiredType: "HASH", reportedType: ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired, current := accessMethodPair(test.desiredType, test.reportedType)
			diff := &difftypes.SchemaDiff{}

			compare.IndexesWithDialect(desired, current, diff, test.dialect)

			c.Assert(diff.IndexAdditions(), qt.HasLen, 0)
			c.Assert(diff.IndexRemovals(), qt.HasLen, 0)
		})
	}
}
