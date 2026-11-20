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
		// The inspector writes NO ACTION out, and an HCL document spells it
		// NO_ACTION.
		{name: "a foreign key with NO ACTION written", constraint: schemamodel.Constraint{
			Name: "f", Type: "FOREIGN KEY", Columns: []string{"id", "valid_at"},
			ForeignTable: "p", ForeignColumns: []string{"id", "valid_at"}, Period: true,
			OnDelete: "NO ACTION", OnUpdate: "no action",
		}},
		{name: "a foreign key with NO_ACTION from HCL", constraint: schemamodel.Constraint{
			Name: "f", Type: "FOREIGN KEY", Columns: []string{"id", "valid_at"},
			ForeignTable: "p", ForeignColumns: []string{"id", "valid_at"}, Period: true,
			OnDelete: "NO_ACTION", OnUpdate: "NO_ACTION",
		}},
		{name: "a deferred foreign key", constraint: schemamodel.Constraint{
			Name: "f", Type: "FOREIGN KEY", Columns: []string{"id", "valid_at"},
			ForeignTable: "p", ForeignColumns: []string{"id", "valid_at"}, Period: true,
			Deferrable: true, Initially: "DEFERRED",
		}},
		// A plain foreign key keeps every action.
		{name: "a plain foreign key that cascades", constraint: schemamodel.Constraint{
			Name: "f", Type: "FOREIGN KEY", Columns: []string{"id", "valid_at"},
			ForeignTable: "p", ForeignColumns: []string{"id", "valid_at"}, OnDelete: "CASCADE",
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
		{
			name: "period with ON DELETE CASCADE",
			constraint: schemamodel.Constraint{
				Name: "f", Type: "FOREIGN KEY", Columns: []string{"a", "b"},
				ForeignTable: "p", ForeignColumns: []string{"a", "b"}, Period: true, OnDelete: "cascade",
			},
			want: `constraint "f" has period and ON DELETE CASCADE; a PERIOD foreign key takes only NO ACTION`,
		},
		{
			name: "period with ON UPDATE RESTRICT",
			constraint: schemamodel.Constraint{
				Name: "f", Type: "FOREIGN KEY", Columns: []string{"a", "b"},
				ForeignTable: "p", ForeignColumns: []string{"a", "b"}, Period: true, OnUpdate: "RESTRICT",
			},
			want: `constraint "f" has period and ON UPDATE RESTRICT; a PERIOD foreign key takes only NO ACTION`,
		},
		{
			name: "period with SET_NULL from HCL",
			constraint: schemamodel.Constraint{
				Name: "f", Type: "FOREIGN KEY", Columns: []string{"a", "b"},
				ForeignTable: "p", ForeignColumns: []string{"a", "b"}, Period: true, OnDelete: "SET_NULL",
			},
			want: `constraint "f" has period and ON DELETE SET NULL; a PERIOD foreign key takes only NO ACTION`,
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
	// A model may carry the key as parts alone.
	c.Assert(temporalkeys.CheckPrimaryKey(schemamodel.Table{
		Name:                      "t",
		PrimaryKeyParts:           []schemamodel.PrimaryKeyPart{{Name: "id"}, {Name: "valid_at"}},
		PrimaryKeyWithoutOverlaps: true,
	}), qt.IsNil)
}

func TestCheckPrimaryKey_FailurePath(t *testing.T) {
	c := qt.New(t)

	err := temporalkeys.CheckPrimaryKey(schemamodel.Table{
		Name: "t", PrimaryKey: []string{"valid_at"}, PrimaryKeyWithoutOverlaps: true,
	})
	c.Assert(err, qt.ErrorMatches, `table "t" has primary_key_without_overlaps and a primary_key of fewer than two columns.*`)

	err = temporalkeys.CheckPrimaryKey(schemamodel.Table{
		Name:                      "t",
		PrimaryKeyParts:           []schemamodel.PrimaryKeyPart{{Name: "valid_at"}},
		PrimaryKeyWithoutOverlaps: true,
	})
	c.Assert(err, qt.ErrorMatches, `table "t" has primary_key_without_overlaps and a primary_key of fewer than two columns.*`)
}

func TestFlag_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		attributes map[string]string
		want       bool
	}{
		{name: "absent", attributes: make(map[string]string), want: false},
		{name: "true", attributes: map[string]string{"period": "true"}, want: true},
		{name: "false", attributes: map[string]string{"period": "false"}, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := temporalkeys.Flag(test.attributes, "period")

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// A present value that is not exactly true or false is refused rather than
// read as false, which would declare a plain key. Every boolean annotation
// attribute reads only the lowercase true as true, so another case is refused
// too, and so is an empty value, which is present and says neither.
func TestFlag_FailurePath(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  string
	}{
		{name: "empty", value: "", want: `without_overlaps="" is not a boolean; write true or false`},
		{name: "another case", value: "TRUE", want: `without_overlaps="TRUE" is not a boolean; write true or false`},
		{name: "surrounding space", value: " true ", want: `without_overlaps=" true " is not a boolean; write true or false`},
		{name: "yes", value: "yes", want: `without_overlaps="yes" is not a boolean; write true or false`},
		{name: "one", value: "1", want: `without_overlaps="1" is not a boolean; write true or false`},
		{name: "a column name", value: "valid_at", want: `without_overlaps="valid_at" is not a boolean; write true or false`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := temporalkeys.Flag(map[string]string{"without_overlaps": test.value}, "without_overlaps")

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(got, qt.IsFalse)
		})
	}
}
