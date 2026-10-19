package descriptor_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/descriptor"
)

// TestNewRegistry_Grammar_HappyPath pins that a grammar using every field,
// with every value each enumerated field may take, is accepted as given.
func TestNewRegistry_Grammar_HappyPath(t *testing.T) {
	c := qt.New(t)
	described := head("alpha")
	described.Grammar = descriptor.Grammar{
		Columns: descriptor.ColumnGrammar{
			CurrentTimeKeywords: true,
			SecondCheckRefusal:  "alpha takes one CHECK on a column",
			ReferencesRefusal:   "what alpha builds depends on its release",
			Constraint: descriptor.ColumnConstraint{
				Define: descriptor.ConstraintKindReferences,
				Modify: descriptor.ConstraintKindCheck,
			},
			ModifyRefusesReferences: true,
			TypeArgumentsAreTypes:   true,
		},
		Constraints: descriptor.ConstraintGrammar{
			Enforcement: descriptor.Enforcement{Checks: true, ForeignKeys: true, Once: true},
			NotValid:    true,
			Match:       descriptor.Match{Clause: true, Partial: true},
			Deferral: descriptor.Deferral{
				KeyRefusal:           "alpha takes one only after a foreign key",
				LoneInitiallyRefusal: "alpha takes it only after DEFERRABLE",
			},
			SymbolOptional: true,
		},
		Keys: descriptor.KeyGrammar{
			IndexKeywords:            descriptor.IndexKeywordValues(),
			IndexKeywordsNeedColumns: true,
			Options:                  true,
			Visibility: descriptor.KeyVisibility{
				Refused: descriptor.KeyVisibilityClauses(),
				Refusal: "alpha has no visibility",
			},
			BareUniqueName:              true,
			ForeignKeyIndexName:         true,
			ForeignKeyIndexNameNamesKey: true,
			FunctionalParts:             true,
			RefusesNullsDistinct:        true,
		},
		Indexes: descriptor.IndexGrammar{
			TrailingVisibility: true,
			Alter:              descriptor.AlterIndexVisibility,
			DropNamesTable:     true,
		},
		AlterTable: descriptor.AlterTableGrammar{
			AddIndex:              true,
			RenameIndex:           true,
			Modify:                descriptor.ModifyOther,
			AlterColumnDefinition: true,
			ChecksCapabilities:    true,
			ColumnGuards:          true,
		},
		Statements: descriptor.StatementGrammar{
			UseSelectsDatabase: true,
			Extensions:         true,
			UserTypes:          true,
			GrantTargetLists:   true,
			RoleIfNotExists:    true,
		},
		Routines: descriptor.RoutineGrammar{
			Body:                   descriptor.RoutineQuotedBody,
			TriggerConditions:      true,
			TriggerStatementBody:   true,
			DefaultDoBlockLanguage: "plalpha",
		},
	}

	registry, err := descriptor.NewRegistry(described)

	c.Assert(err, qt.IsNil)
	got, found := registry.Lookup("alpha")
	c.Assert(found, qt.IsTrue)
	c.Assert(got.Grammar, qt.DeepEquals, described.Grammar)
}

func TestNewRegistry_Grammar_FailurePath(t *testing.T) {
	with := func(change func(*descriptor.Grammar)) descriptor.Dialect {
		described := head("alpha")
		change(&described.Grammar)
		return described
	}
	tests := []struct {
		name    string
		dialect descriptor.Dialect
		wantErr string
	}{
		{
			name:    "an undeclared column constraint kind",
			dialect: with(func(g *descriptor.Grammar) { g.Columns.Constraint.Define = "UNIQUE" }),
			wantErr: `invalid dialect descriptor: dialect "alpha": grammar: column constraint kind "UNIQUE" is not one of .*`,
		},
		{
			name: "an undeclared column constraint kind in MODIFY",
			dialect: with(func(g *descriptor.Grammar) {
				g.Columns.Constraint = descriptor.ColumnConstraint{Define: "CHECK", Modify: "NOT NULL"}
			}),
			wantErr: `invalid dialect descriptor: dialect "alpha": grammar: column constraint kind "NOT NULL" in MODIFY is not one of .*`,
		},
		{
			name:    "a MODIFY kind without a column constraint kind",
			dialect: with(func(g *descriptor.Grammar) { g.Columns.Constraint.Modify = "CHECK" }),
			wantErr: `invalid dialect descriptor: dialect "alpha": grammar: a MODIFY column constraint kind needs a column constraint kind`,
		},
		{
			name:    "a MATCH PARTIAL without the clause",
			dialect: with(func(g *descriptor.Grammar) { g.Constraints.Match.Partial = true }),
			wantErr: `invalid dialect descriptor: dialect "alpha": grammar: a MATCH type or refusal needs the MATCH clause`,
		},
		{
			name:    "a MATCH refusal without the clause",
			dialect: with(func(g *descriptor.Grammar) { g.Constraints.Match.Refusal = "alpha records NONE" }),
			wantErr: `invalid dialect descriptor: dialect "alpha": grammar: a MATCH type or refusal needs the MATCH clause`,
		},
		{
			name: "a MATCH PARTIAL both built and refused",
			dialect: with(func(g *descriptor.Grammar) {
				g.Constraints.Match = descriptor.Match{Clause: true, Partial: true, Refusal: "alpha records NONE"}
			}),
			wantErr: `invalid dialect descriptor: dialect "alpha": grammar: MATCH PARTIAL cannot be both built and refused`,
		},
		{
			name: "a deferral refusal beside a key refusal",
			dialect: with(func(g *descriptor.Grammar) {
				g.Constraints.Deferral = descriptor.Deferral{Refusal: "alpha has none", KeyRefusal: "alpha takes one after a key"}
			}),
			wantErr: `invalid dialect descriptor: dialect "alpha": grammar: a deferral refusal leaves .*`,
		},
		{
			name: "a deferral refusal beside a lone INITIALLY refusal",
			dialect: with(func(g *descriptor.Grammar) {
				g.Constraints.Deferral = descriptor.Deferral{Refusal: "alpha has none", LoneInitiallyRefusal: "alpha needs DEFERRABLE"}
			}),
			wantErr: `invalid dialect descriptor: dialect "alpha": grammar: a deferral refusal leaves .*`,
		},
		{
			name:    "an undeclared index keyword",
			dialect: with(func(g *descriptor.Grammar) { g.Keys.IndexKeywords = []string{"index"} }),
			wantErr: `invalid dialect descriptor: dialect "alpha": grammar: index keyword "index" is not one of .*`,
		},
		{
			name:    "an index keyword written twice",
			dialect: with(func(g *descriptor.Grammar) { g.Keys.IndexKeywords = []string{"KEY", "INDEX", "KEY"} }),
			wantErr: `invalid dialect descriptor: dialect "alpha": grammar: index keyword "KEY" is written twice`,
		},
		{
			name:    "index keywords that need columns without a keyword",
			dialect: with(func(g *descriptor.Grammar) { g.Keys.IndexKeywordsNeedColumns = true }),
			wantErr: `invalid dialect descriptor: dialect "alpha": grammar: index keywords that need columns name no index keyword`,
		},
		{
			name: "an undeclared visibility clause",
			dialect: with(func(g *descriptor.Grammar) {
				g.Keys.Visibility = descriptor.KeyVisibility{Refused: []string{"HIDDEN"}, Refusal: "alpha hides nothing"}
			}),
			wantErr: `invalid dialect descriptor: dialect "alpha": grammar: visibility clause "HIDDEN" is not one of .*`,
		},
		{
			name:    "a refused visibility clause without its refusal",
			dialect: with(func(g *descriptor.Grammar) { g.Keys.Visibility.Refused = []string{"INVISIBLE"} }),
			wantErr: `invalid dialect descriptor: dialect "alpha": grammar: a refused visibility clause and its refusal are set together`,
		},
		{
			name:    "a visibility refusal without a refused clause",
			dialect: with(func(g *descriptor.Grammar) { g.Keys.Visibility.Refusal = "alpha hides nothing" }),
			wantErr: `invalid dialect descriptor: dialect "alpha": grammar: a refused visibility clause and its refusal are set together`,
		},
		{
			name:    "a foreign key index name that names the key where no such name is read",
			dialect: with(func(g *descriptor.Grammar) { g.Keys.ForeignKeyIndexNameNamesKey = true }),
			wantErr: `invalid dialect descriptor: dialect "alpha": grammar: a foreign key index name that names the key ` +
				`is stated, and no foreign key index name is read`,
		},
		{
			name:    "an undeclared ALTER INDEX grammar",
			dialect: with(func(g *descriptor.Grammar) { g.Indexes.Alter = "set-tablespace" }),
			wantErr: `invalid dialect descriptor: dialect "alpha": grammar: ALTER INDEX grammar "set-tablespace" is not one of .*`,
		},
		{
			name:    "an undeclared MODIFY grammar",
			dialect: with(func(g *descriptor.Grammar) { g.AlterTable.Modify = "replace" }),
			wantErr: `invalid dialect descriptor: dialect "alpha": grammar: MODIFY grammar "replace" is not one of .*`,
		},
		{
			name:    "an undeclared routine body grammar",
			dialect: with(func(g *descriptor.Grammar) { g.Routines.Body = "plpgsql" }),
			wantErr: `invalid dialect descriptor: dialect "alpha": grammar: routine body grammar "plpgsql" is not one of .*`,
		},
		{
			name: "a default DO-block language with a grammar that reads no DO block",
			dialect: with(func(g *descriptor.Grammar) {
				g.Routines = descriptor.RoutineGrammar{Body: descriptor.RoutineTransactSQL, DefaultDoBlockLanguage: "plalpha"}
			}),
			wantErr: `invalid dialect descriptor: dialect "alpha": grammar: default DO-block language "plalpha" is stated, ` +
				`and routine grammar "transact-sql" reads no DO block`,
		},
		{
			name:    "a default DO-block language with no routine grammar",
			dialect: with(func(g *descriptor.Grammar) { g.Routines.DefaultDoBlockLanguage = "plalpha" }),
			wantErr: `invalid dialect descriptor: dialect "alpha": grammar: default DO-block language "plalpha" is stated, ` +
				`and routine grammar "" reads no DO block`,
		},
		{
			name: "an upper-case default DO-block language",
			dialect: with(func(g *descriptor.Grammar) {
				g.Routines = descriptor.RoutineGrammar{Body: descriptor.RoutineQuotedBody, DefaultDoBlockLanguage: "PLalpha"}
			}),
			wantErr: `invalid dialect descriptor: dialect "alpha": grammar: default DO-block language "PLalpha" is not one lower-case word`,
		},
		{
			name: "a default DO-block language of two words",
			dialect: with(func(g *descriptor.Grammar) {
				g.Routines = descriptor.RoutineGrammar{Body: descriptor.RoutineQuotedBody, DefaultDoBlockLanguage: "pl alpha"}
			}),
			wantErr: `invalid dialect descriptor: dialect "alpha": grammar: default DO-block language "pl alpha" is not one lower-case word`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			registry, err := descriptor.NewRegistry(test.dialect)

			c.Assert(err, qt.ErrorIs, descriptor.ErrInvalidDescriptor)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(registry, qt.IsNil)
		})
	}
}
