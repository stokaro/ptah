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
