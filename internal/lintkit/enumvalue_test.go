package lintkit_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/lintkit"
)

// An ADD VALUE and a RENAME VALUE are read with the type, the label and the
// guard as written.
func TestAlterEnumValue_HappyPath(t *testing.T) {
	for _, test := range []struct {
		sql  string
		want lintkit.EnumValueChange
	}{
		{"ALTER TYPE mood ADD VALUE 'c'", lintkit.EnumValueChange{
			Type: lintkit.TableReference{Name: "mood", Normalized: "MOOD"}, Label: "c", Adds: true,
		}},
		{"ALTER TYPE public.mood ADD VALUE IF NOT EXISTS 'c' BEFORE 'a'", lintkit.EnumValueChange{
			Type: lintkit.TableReference{Name: "public.mood", Normalized: "PUBLIC.MOOD"}, Label: "c", Adds: true, IfNotExists: true,
		}},
		{"ALTER TYPE mood ADD VALUE 'it''s'", lintkit.EnumValueChange{
			Type: lintkit.TableReference{Name: "mood", Normalized: "MOOD"}, Label: "it's", Adds: true,
		}},
		{`ALTER TYPE "Mood" RENAME VALUE 'a' TO 'z'`, lintkit.EnumValueChange{
			Type: lintkit.TableReference{Name: `"Mood"`, Normalized: "MOOD"}, Label: "a",
		}},
	} {
		t.Run(test.sql, func(t *testing.T) {
			c := qt.New(t)
			statement := statements(test.sql)[0]
			got, ok := lintkit.AlterEnumValue(statement.Words, statement.SourceWords)
			c.Assert(ok, qt.IsTrue)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// Any other statement, and a label in a spelling the words do not decode, is
// not read.
func TestAlterEnumValue_FailurePath(t *testing.T) {
	for _, sql := range []string{
		"ALTER TYPE mood RENAME TO feeling",
		"ALTER TYPE address ADD ATTRIBUTE zip text",
		"ALTER TYPE mood ADD VALUE E'c'",
		"ALTER TYPE mood ADD VALUE",
		"ALTER TABLE mood ADD VALUE 'c'",
		"ALTER TYPE",
	} {
		t.Run(sql, func(t *testing.T) {
			c := qt.New(t)
			statement := statements(sql)[0]
			got, ok := lintkit.AlterEnumValue(statement.Words, statement.SourceWords)
			c.Assert(ok, qt.IsFalse)
			c.Assert(got, qt.DeepEquals, lintkit.EnumValueChange{})
		})
	}
}
