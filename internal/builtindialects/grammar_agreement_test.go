package builtindialects_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/google/go-cmp/cmp/cmpopts"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/builtindialects"
)

// oldParserSwitches is what the dialect switches of the shared DDL parser,
// internal/parser, answered for each engine before the descriptors carried
// the grammar, written out here as they stood:
//
//   - takesOneEnforcement, takesEnforced and takesNotValid: PostgreSQL took one
//     [NOT] ENFORCED after a CHECK or a foreign key, MySQL any number after a
//     CHECK, and PostgreSQL, CockroachDB and YugabyteDB took NOT VALID.
//   - takesMatch and refuseMatchType: the PostgreSQL family without Spanner,
//     MySQL, MariaDB and SQLite took MATCH; MySQL built PARTIAL, MariaDB and
//     SQLite refused FULL and PARTIAL with their own sentences, and the rest
//     refused PARTIAL.
//   - parseDeferral, parseInitially, applyToKey and refuseOnCheck: MySQL and
//     MariaDB refused every deferral clause with one sentence and CockroachDB
//     with another; SQLite refused one after a key or a CHECK, and an
//     INITIALLY with no DEFERRABLE before it.
//   - admitSymbolLessTableConstraint, handleColumnCheck, refuseColumnReferences
//     and mysqlColumnConstraintKinds: the MySQL family took CONSTRAINT without
//     a name; MariaDB refused a second column CHECK, MySQL a column-level
//     REFERENCES; a column CONSTRAINT preceded CHECK alone on MySQL, in a
//     MODIFY too, and REFERENCES alone on MariaDB, nothing in a MODIFY, and
//     neither took REFERENCES in a MODIFY.
//   - handleDefaultKeyword and refuseTableElementTypo: the PostgreSQL family
//     kept the current-time keywords bare, and ClickHouse's type arguments were
//     exempt from the typo refusal.
//   - tableBodyReadsKeywordAsIndex: the MySQL family read all four index words
//     as an index; SQLite and Oracle INDEX; SQL Server INDEX and KEY;
//     CockroachDB INDEX before a column list; PostgreSQL, YugabyteDB, Spanner
//     and ClickHouse none.
//   - readsKeyOptions, readKeyVisibility, the bare UNIQUE name, the FOREIGN KEY
//     index name and parseNullsDistinctClause read the MySQL family; MySQL
//     refused IGNORED and NOT IGNORED, MariaDB INVISIBLE; parseFunctionalKeyPart
//     read MySQL alone; foreignKeyBackingIndex, through
//     mysqlname.ClauseIndexNamesKey, read the FOREIGN KEY index name as the
//     key's own on MariaDB alone.
//   - routineParser read the MySQL family's routines, SQL Server's and the
//     PostgreSQL family's, and isPostgresRoutineDialect read the PostgreSQL
//     family's DO blocks, routine bodies, string defaults, and trigger WHEN
//     and REFERENCING; collectTriggerBody took the MySQL family's one-statement
//     trigger body; CREATE OR ALTER was SQL Server's. parsePostgresDoBlockSQL
//     gave a DO block that writes no LANGUAGE the language plpgsql on every
//     dialect whose DO blocks it read.
//   - parseUse, CREATE ROLE IF NOT EXISTS, ADD KEY or INDEX, RENAME INDEX,
//     MODIFY, refuseDropCheck and refuseAlterGuard read the MySQL family, the
//     column guard MariaDB alone; refuseModify called Oracle's and ClickHouse's
//     MODIFY not MySQL's; ALTER COLUMN stated a definition on SQL Server.
//   - refuseCreateExtension and GRANT target lists read the PostgreSQL family,
//     refuseUserType the family and Oracle.
//   - ALTER INDEX renamed on PostgreSQL and changed visibility on CockroachDB,
//     whose indexes alone end with a visibility; SQL Server alone read DROP
//     INDEX t.idx as table.index.
//
// YDB's documents never reach the shared grammar, which refuses the YQL
// lexer before any of the switches is asked, so it states no grammar.
var oldParserSwitches = map[string]descriptor.Grammar{
	platform.Postgres: {
		Columns: descriptor.ColumnGrammar{CurrentTimeKeywords: true, StoredStringDefaults: true},
		Constraints: descriptor.ConstraintGrammar{
			Enforcement: descriptor.Enforcement{Checks: true, ForeignKeys: true, Once: true},
			NotValid:    true,
			Match:       descriptor.Match{Clause: true},
		},
		Indexes:    descriptor.IndexGrammar{Alter: descriptor.AlterIndexRename},
		Statements: descriptor.StatementGrammar{Extensions: true, UserTypes: true, GrantTargetLists: true},
		Routines: descriptor.RoutineGrammar{
			Body: descriptor.RoutineQuotedBody, TriggerConditions: true, DefaultDoBlockLanguage: "plpgsql",
		},
	},
	platform.MySQL: {
		Columns: descriptor.ColumnGrammar{
			ReferencesRefusal: "MySQL 8.4 builds nothing from the clause, while MySQL 9.7 and 26.7 build a foreign " +
				"key and its index, and the SQL file is read without the server version, so Ptah refuses the " +
				"clause rather than guess which schema it declares; write a table-level FOREIGN KEY clause, " +
				"which every MySQL line builds",
			Constraint:              descriptor.ColumnConstraint{Define: "CHECK", Modify: "CHECK"},
			ModifyRefusesReferences: true,
		},
		Constraints: descriptor.ConstraintGrammar{
			Enforcement: descriptor.Enforcement{Checks: true},
			Match:       descriptor.Match{Clause: true, Partial: true},
			Deferral: descriptor.Deferral{Refusal: "MySQL and MariaDB have no deferrable constraints, and MySQL 8.4 " +
				"and MariaDB 11.8 answer ERROR 1064 to the clause"},
			SymbolOptional: true,
		},
		Keys: descriptor.KeyGrammar{
			IndexKeywords: []string{"KEY", "INDEX", "FULLTEXT", "SPATIAL"},
			Options:       true,
			Visibility: descriptor.KeyVisibility{
				Refused: []string{"IGNORED", "NOT IGNORED"},
				Refusal: "it is MariaDB's clause, and MySQL 8.4 answers ERROR 1064",
			},
			BareUniqueName:       true,
			ForeignKeyIndexName:  true,
			FunctionalParts:      true,
			RefusesNullsDistinct: true,
		},
		AlterTable: descriptor.AlterTableGrammar{
			AddIndex: true, RenameIndex: true, Modify: descriptor.ModifyDefinition, ChecksCapabilities: true,
		},
		Statements: descriptor.StatementGrammar{UseSelectsDatabase: true, RoleIfNotExists: true},
		Routines:   descriptor.RoutineGrammar{Body: descriptor.RoutineCompoundStatement, TriggerStatementBody: true},
	},
	platform.MariaDB: {
		Columns: descriptor.ColumnGrammar{
			SecondCheckRefusal: "MariaDB accepts one CHECK on a column and answers ERROR 1064 (42000) to the " +
				"second; write it at table level",
			Constraint:              descriptor.ColumnConstraint{Define: "REFERENCES"},
			ModifyRefusesReferences: true,
		},
		Constraints: descriptor.ConstraintGrammar{
			Match: descriptor.Match{Clause: true, Refusal: "MariaDB 11.8.9 accepts the clause and records NONE, " +
				"so the key it builds is MATCH SIMPLE; declare the key without the clause"},
			Deferral: descriptor.Deferral{Refusal: "MySQL and MariaDB have no deferrable constraints, and MySQL 8.4 " +
				"and MariaDB 11.8 answer ERROR 1064 to the clause"},
			SymbolOptional: true,
		},
		Keys: descriptor.KeyGrammar{
			IndexKeywords: []string{"KEY", "INDEX", "FULLTEXT", "SPATIAL"},
			Options:       true,
			Visibility: descriptor.KeyVisibility{
				Refused: []string{"INVISIBLE"},
				Refusal: "it is MySQL's clause, and MariaDB 11.8.9 answers ERROR 1064; MariaDB spells it IGNORED",
			},
			BareUniqueName:              true,
			ForeignKeyIndexName:         true,
			ForeignKeyIndexNameNamesKey: true,
			RefusesNullsDistinct:        true,
		},
		AlterTable: descriptor.AlterTableGrammar{
			AddIndex: true, RenameIndex: true, Modify: descriptor.ModifyDefinition, ChecksCapabilities: true, ColumnGuards: true,
		},
		Statements: descriptor.StatementGrammar{UseSelectsDatabase: true, RoleIfNotExists: true},
		Routines:   descriptor.RoutineGrammar{Body: descriptor.RoutineCompoundStatement, TriggerStatementBody: true},
	},
	platform.ClickHouse: {
		Columns:    descriptor.ColumnGrammar{TypeArgumentsAreTypes: true},
		AlterTable: descriptor.AlterTableGrammar{Modify: descriptor.ModifyOther},
	},
	platform.SQLite: {
		Constraints: descriptor.ConstraintGrammar{
			Match: descriptor.Match{Clause: true, Refusal: "SQLite 3.51 accepts the clause and records NONE, so " +
				"the key it builds is MATCH SIMPLE; declare the key without the clause"},
			Deferral: descriptor.Deferral{
				KeyRefusal:           "SQLite takes one only after a foreign key",
				LoneInitiallyRefusal: "SQLite takes it only after [NOT] DEFERRABLE",
			},
		},
		Keys: descriptor.KeyGrammar{IndexKeywords: []string{"INDEX"}},
	},
	platform.SQLServer: {
		Keys:       descriptor.KeyGrammar{IndexKeywords: []string{"KEY", "INDEX"}},
		Indexes:    descriptor.IndexGrammar{DropNamesTable: true},
		AlterTable: descriptor.AlterTableGrammar{AlterColumnDefinition: true},
		Routines:   descriptor.RoutineGrammar{Body: descriptor.RoutineTransactSQL},
	},
	platform.CockroachDB: {
		Columns: descriptor.ColumnGrammar{CurrentTimeKeywords: true, StoredStringDefaults: true},
		Constraints: descriptor.ConstraintGrammar{
			NotValid: true,
			Match:    descriptor.Match{Clause: true},
			Deferral: descriptor.Deferral{Refusal: "CockroachDB has no deferrable constraints, and v26.3.2 refuses " +
				"the clause, NOT DEFERRABLE included"},
		},
		Keys:       descriptor.KeyGrammar{IndexKeywords: []string{"INDEX"}, IndexKeywordsNeedColumns: true},
		Indexes:    descriptor.IndexGrammar{TrailingVisibility: true, Alter: descriptor.AlterIndexVisibility},
		Statements: descriptor.StatementGrammar{Extensions: true, UserTypes: true, GrantTargetLists: true},
		Routines: descriptor.RoutineGrammar{
			Body: descriptor.RoutineQuotedBody, TriggerConditions: true, DefaultDoBlockLanguage: "plpgsql",
		},
	},
	platform.YugabyteDB: {
		Columns: descriptor.ColumnGrammar{CurrentTimeKeywords: true, StoredStringDefaults: true},
		Constraints: descriptor.ConstraintGrammar{
			NotValid: true,
			Match:    descriptor.Match{Clause: true},
		},
		Statements: descriptor.StatementGrammar{Extensions: true, UserTypes: true, GrantTargetLists: true},
		Routines: descriptor.RoutineGrammar{
			Body: descriptor.RoutineQuotedBody, TriggerConditions: true, DefaultDoBlockLanguage: "plpgsql",
		},
	},
	platform.Spanner: {
		Columns:    descriptor.ColumnGrammar{CurrentTimeKeywords: true, StoredStringDefaults: true},
		Statements: descriptor.StatementGrammar{Extensions: true, UserTypes: true, GrantTargetLists: true},
		Routines: descriptor.RoutineGrammar{
			Body: descriptor.RoutineQuotedBody, TriggerConditions: true, DefaultDoBlockLanguage: "plpgsql",
		},
	},
	platform.Oracle: {
		Keys:       descriptor.KeyGrammar{IndexKeywords: []string{"INDEX"}},
		AlterTable: descriptor.AlterTableGrammar{Modify: descriptor.ModifyOther},
		Statements: descriptor.StatementGrammar{UserTypes: true},
	},
	platform.YDB: {},
}

// TestDescriptors_GrammarAgreesWithTheParserSwitches pins every built-in
// descriptor's grammar, through every spelling that selects it, to what the
// parser's dialect switches answered.
func TestDescriptors_GrammarAgreesWithTheParserSwitches(t *testing.T) {
	c := qt.New(t)
	c.Assert(builtindialects.Descriptors(), qt.HasLen, len(oldParserSwitches))
	for _, spelling := range platform.DialectSpellings() {
		t.Run(spelling, func(t *testing.T) {
			c := qt.New(t)

			got, found := platform.Lookup(spelling)

			c.Assert(found, qt.IsTrue)
			want, pinned := oldParserSwitches[got.Name]
			c.Assert(pinned, qt.IsTrue)
			c.Assert(got.Grammar, qt.CmpEquals(cmpopts.EquateEmpty()), want)
		})
	}
}

// TestDescriptors_ParserReadsTheFactsTheSwitchesShared pins the facts the
// parser reads from other sections, which stated them before the grammar did:
// ClickHouse alone keeps nullability in the type, MySQL alone refuses a column
// CHECK naming another column, and a routine of PostgreSQL's grammar is read
// with the lexer of the family's head, PostgreSQL.
func TestDescriptors_ParserReadsTheFactsTheSwitchesShared(t *testing.T) {
	for _, described := range builtindialects.Descriptors() {
		t.Run(described.Name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(described.Comparison.NullableTypes, qt.Equals, described.Name == platform.ClickHouse)
			c.Assert(described.Columns.CheckOwnColumnOnly, qt.Equals, described.Name == platform.MySQL)
			c.Assert(described.Family == platform.Postgres, qt.Equals,
				described.Grammar.Routines.Body == descriptor.RoutineQuotedBody)
		})
	}
}
