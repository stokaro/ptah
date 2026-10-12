package mysqlindex_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/internal/mysqlindex"
)

// TestConstraintBlockSize_HappyPath accepts a hint on a PRIMARY KEY
// constraint in any case and with surrounding space, and a hint of zero on
// any constraint, since zero states none.
func TestConstraintBlockSize_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name, constraintType string
		size                 uint64
	}{
		{"a primary key", "PRIMARY KEY", 8},
		{"a primary key in lower case", "primary key", 8},
		{"a primary key with spaces", " Primary Key ", 8},
		{"a UNIQUE constraint without a hint", "UNIQUE", 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(mysqlindex.ConstraintBlockSize(test.constraintType, "k", test.size), qt.IsNil)
		})
	}
}

// TestConstraintBlockSize_FailurePath refuses a hint on any other constraint
// and names the type as given, which the renderer and the Go annotation
// reader both pass in capitals.
func TestConstraintBlockSize_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name, constraintType, wantErr string
	}{
		{"a UNIQUE constraint", "UNIQUE",
			`unsupported feature: KEY_BLOCK_SIZE on UNIQUE constraint "k" is not supported; declare a unique index for a UNIQUE key`},
		{"a foreign key", "FOREIGN KEY",
			`unsupported feature: KEY_BLOCK_SIZE on FOREIGN KEY constraint "k" is not supported; declare a unique index for a UNIQUE key`},
		{"a key that is almost primary", "PRIMARY",
			`unsupported feature: KEY_BLOCK_SIZE on PRIMARY constraint "k" is not supported; declare a unique index for a UNIQUE key`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := mysqlindex.ConstraintBlockSize(test.constraintType, "k", 8)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}
