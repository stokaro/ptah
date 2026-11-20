package temporalkeys_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/temporalkeys"
)

func TestCheckConstraint_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		constraint schemamodel.Constraint
	}{
		{name: "a plain key carries neither clause", constraint: schemamodel.Constraint{
			Type: "UNIQUE", Columns: []string{"a"},
		}},
		{name: "a unique key", constraint: schemamodel.Constraint{
			Name: "u", Type: "UNIQUE", Columns: []string{"id", "valid_at"}, WithoutOverlaps: true,
		}},
		{name: "a primary key", constraint: schemamodel.Constraint{
			Name: "p", Type: "PRIMARY KEY", Columns: []string{"id", "valid_at"}, WithoutOverlaps: true,
		}},
		{name: "a foreign key", constraint: schemamodel.Constraint{
			Name: "f", Type: "FOREIGN KEY", Columns: []string{"id", "valid_at"},
			ForeignTable: "p", ForeignColumns: []string{"id", "valid_at"}, Period: true,
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(temporalkeys.CheckConstraint(test.constraint), qt.IsNil)
		})
	}
}

func TestCheckConstraint_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		constraint schemamodel.Constraint
		want       string
	}{
		{
			name: "without_overlaps on a foreign key",
			constraint: schemamodel.Constraint{
				Name: "f", Type: "FOREIGN KEY", Columns: []string{"a", "b"}, WithoutOverlaps: true,
			},
			want: `constraint "f" is a FOREIGN KEY, and without_overlaps belongs to a PRIMARY KEY or UNIQUE constraint`,
		},
		{
			name: "without_overlaps on one column",
			constraint: schemamodel.Constraint{
				Name: "u", Type: "UNIQUE", Columns: []string{"valid_at"}, WithoutOverlaps: true,
			},
			want: `constraint "u" has without_overlaps and fewer than two columns`,
		},
		{
			name: "period on a unique constraint",
			constraint: schemamodel.Constraint{
				Name: "u", Type: "UNIQUE", Columns: []string{"a", "b"}, Period: true,
			},
			want: `constraint "u" is a UNIQUE, and period belongs to a FOREIGN KEY constraint`,
		},
		{
			name: "period on one column",
			constraint: schemamodel.Constraint{
				Name: "f", Type: "FOREIGN KEY", Columns: []string{"valid_at"},
				ForeignTable: "p", ForeignColumns: []string{"valid_at"}, Period: true,
			},
			want: `constraint "f" has period and needs the same number of columns, at least two, on each side`,
		},
		{
			name: "period with unequal sides",
			constraint: schemamodel.Constraint{
				Name: "f", Type: "FOREIGN KEY", Columns: []string{"a", "b"},
				ForeignTable: "p", ForeignColumns: []string{"a"}, Period: true,
			},
			want: `constraint "f" has period and needs the same number of columns, at least two, on each side`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := temporalkeys.CheckConstraint(test.constraint)

			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}

func TestCheckPrimaryKey_HappyPath(t *testing.T) {
	c := qt.New(t)

	c.Assert(temporalkeys.CheckPrimaryKey(schemamodel.Table{Name: "t", PrimaryKey: []string{"a"}}), qt.IsNil)
	c.Assert(temporalkeys.CheckPrimaryKey(schemamodel.Table{
		Name: "t", PrimaryKey: []string{"id", "valid_at"}, PrimaryKeyWithoutOverlaps: true,
	}), qt.IsNil)
}

func TestCheckPrimaryKey_FailurePath(t *testing.T) {
	c := qt.New(t)

	err := temporalkeys.CheckPrimaryKey(schemamodel.Table{
		Name: "t", PrimaryKey: []string{"valid_at"}, PrimaryKeyWithoutOverlaps: true,
	})

	c.Assert(err, qt.ErrorMatches, `table "t" has primary_key_without_overlaps and a primary_key of fewer than two columns.*`)
}

func TestFlag_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  bool
	}{
		{name: "absent", value: "", want: false},
		{name: "true", value: "true", want: true},
		{name: "true in another case, with space", value: " TRUE ", want: true},
		{name: "false", value: "false", want: false},
		{name: "false in another case", value: "False", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := temporalkeys.Flag("period", test.value)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// A value that is neither true nor false is refused rather than read as false,
// which would declare a plain key.
func TestFlag_FailurePath(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  string
	}{
		{name: "yes", value: "yes", want: `without_overlaps="yes" is not a boolean; write true or false`},
		{name: "one", value: "1", want: `without_overlaps="1" is not a boolean; write true or false`},
		{name: "a column name", value: "valid_at", want: `without_overlaps="valid_at" is not a boolean; write true or false`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := temporalkeys.Flag("without_overlaps", test.value)

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(got, qt.IsFalse)
		})
	}
}
