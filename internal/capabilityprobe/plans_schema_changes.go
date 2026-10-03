package capabilityprobe

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
)

// withSchemaChanges adds the questions about how a target holds a table and
// changes one it already holds: whether a key is required, and which ALTERs
// work in place.
//
// Every ALTER is judged by its effect, not by its acceptance. A row that only
// the changed shape admits is written after the change, and where the setup can
// show the old shape refusing the same row first, it does. Accepting an ALTER
// and leaving the table as it was is a server Ptah cannot plan with, so that
// outcome reads false with the evidence beside it rather than true.
//
// The keys name what the engine does. The YDB planner reads them to decide
// what it may emit; the planners that already exist keep their own paths
// (SQLite rebuilds a table whatever these say), and the registry answers for
// every dialect so the comparison is the same question everywhere.
func withSchemaChanges(p plan, dialect string) plan {
	changes, undecided := schemaChangesFor(dialect)
	p.experiments = append(p.experiments, changes.experiments()...)
	for key, reason := range undecided {
		if p.undecided == nil {
			p.undecided = make(map[capability.Capability]string)
		}
		p.undecided[key] = reason
	}
	return p
}

// schemaChangesFor returns one dialect's spelling of the schema-change
// experiments, and the keys it declares instead of asking.
func schemaChangesFor(dialect string) (schemaChanges, map[capability.Capability]string) {
	switch dialect {
	case platform.MySQL, platform.MariaDB:
		return mysqlSchemaChanges(), nil
	case platform.ClickHouse:
		return clickHouseSchemaChanges(), nil
	case platform.SQLite:
		return sqliteSchemaChanges(), nil
	case platform.SQLServer:
		return sqlServerSchemaChanges(), map[capability.Capability]string{
			capability.IndexCoveringColumns: "the server takes INCLUDE on an index, and Ptah's SQL Server " +
				"renderer does not emit it, so the key stays false until the renderer, the reader and the " +
				"planner carry the payload; asking the server would answer a different question",
		}
	case platform.Oracle:
		return oracleSchemaChanges(), nil
	case platform.Spanner:
		return spannerSchemaChanges(), nil
	case platform.YDB:
		return ydbSchemaChanges(), nil
	default:
		return postgresSchemaChanges(), nil
	}
}

// schemaChanges is one dialect's spelling of the experiments. Every table
// carries a primary key, which the keyed dialects require and the others
// accept, so one spelling serves a family.
type schemaChanges struct {
	// keyed and keyless are the same table with and without a primary key.
	keyed, keyless string

	keyChange schemaChange
	retype    schemaChange
	setNull   schemaChange
	dropNull  schemaChange
	defaults  schemaChange
	addColumn schemaChange

	// literalDefault is the control for exprDefault: the same table with a
	// literal default, which every target takes.
	literalDefault string
	exprDefault    schemaChange

	check string

	coverSetup []string
	cover      string
	// coverRead proves the payload columns the cover statement asked for are
	// the index's, where acceptance alone could hide a dropped clause. Nil
	// leaves the key to acceptance.
	coverRead []check

	uniqueIndex schemaChange
}

// schemaChange is one ALTER and the evidence around it.
type schemaChange struct {
	setup []string
	// before must hold on the shape the setup created. A failure there means
	// the server does not behave the way the evidence after the change
	// assumes, so the key is undecidable rather than answered.
	before []check
	change []string
	// after must hold once the change was accepted.
	after []check
}

// check is one statement whose outcome is evidence.
type check struct {
	statement string
	// refused inverts an execution check: the statement must be refused.
	refused bool
	// counted makes the statement a single-value query that must answer want.
	counted bool
	want    int64
	// either runs the statement for its effect and reads neither outcome as
	// evidence; the check after it says what the effect was.
	either bool
	// inspect, when set, is the whole check: a read the session makes some
	// other way than one statement -- through Ptah's own schema reader, where
	// no SQL describes the object -- with whether it held and what it found.
	inspect func(ctx context.Context, s *session) (attempt Attempt, held bool, did string)
	// describes says what inspect expects, for the note when it does not hold.
	describes string
}

func accepts(statement string) check  { return check{statement: statement} }
func attempts(statement string) check { return check{statement: statement, either: true} }
func refuses(statement string) check  { return check{statement: statement, refused: true} }
func counts(query string, want int64) check {
	return check{statement: query, counted: true, want: want}
}

// run executes the check and says whether it held, and what the server did in
// words a note can quote.
func (c check) run(ctx context.Context, s *session) (attempt Attempt, held bool, did string) {
	if c.inspect != nil {
		return c.inspect(ctx, s)
	}
	if c.counted {
		value, attempt := s.query(ctx, c.statement)
		if !attempt.Accepted {
			return attempt, false, "refused"
		}
		return attempt, value == c.want, fmt.Sprintf("answered %d", value)
	}
	attempt = s.exec(ctx, c.statement)
	return attempt, c.either || attempt.Accepted != c.refused, outcome(attempt)
}

func (c check) expectation() string {
	switch {
	case c.inspect != nil:
		return c.describes
	case c.either:
		return fmt.Sprintf("%q to run", collapse(c.statement))
	case c.counted:
		return fmt.Sprintf("%q to answer %d", collapse(c.statement), c.want)
	case c.refused:
		return fmt.Sprintf("%q to be refused", collapse(c.statement))
	default:
		return fmt.Sprintf("%q to be accepted", collapse(c.statement))
	}
}

func (sc schemaChanges) experiments() []experiment {
	out := []experiment{
		enforced(capability.PrimaryKeyRequired, nil, sc.keyed, sc.keyless),
		proven(capability.PrimaryKeyAlterable, sc.keyChange),
		proven(capability.AlterColumnType, sc.retype),
		proven(capability.AlterColumnSetNotNull, sc.setNull),
		proven(capability.AlterColumnDropNotNull, sc.dropNull),
		proven(capability.AlterColumnDefault, sc.defaults),
		proven(capability.AddColumnWithDefault, sc.addColumn),
		proven(capability.ExpressionDefaults, schemaChange{
			before: []check{accepts(sc.literalDefault)},
			change: sc.exprDefault.change,
			after:  sc.exprDefault.after,
		}),
		acceptance(capability.CheckConstraints, nil, sc.check),
		proven(capability.UniqueIndexOnExistingTable, sc.uniqueIndex),
	}
	if sc.cover != "" {
		out = append(out, proven(capability.IndexCoveringColumns, schemaChange{
			setup:  sc.coverSetup,
			change: []string{sc.cover},
			after:  sc.coverRead,
		}))
	}
	return out
}

// proven decides a key by making a change and then using what the change
// claims to have made.
func proven(key capability.Capability, sc schemaChange) experiment {
	return experiment{
		decides: []capability.Capability{key},
		setup:   sc.setup,
		decide: func(ctx context.Context, s *session) (verdicts, []Attempt) {
			var attempts []Attempt
			for _, control := range sc.before {
				attempt, held, did := control.run(ctx, s)
				attempts = append(attempts, attempt)
				if !held {
					return verdicts{key: cannotDecide(
						"before the change the server did not behave as the evidence after it assumes: "+
							"expected %s, and it %s",
						control.expectation(), did,
					)}, attempts
				}
			}
			changed, accepted := s.runAll(ctx, sc.change)
			attempts = append(attempts, changed...)
			if !accepted {
				return verdicts{key: decided(false)}, attempts
			}
			for _, evidence := range sc.after {
				attempt, held, did := evidence.run(ctx, s)
				attempts = append(attempts, attempt)
				if !held {
					return verdicts{key: annotated(false,
						"the change was accepted and did not take effect: expected "+evidence.expectation()+
							", and it "+did,
					)}, attempts
				}
			}
			return verdicts{key: decided(true)}, attempts
		},
	}
}

// postgresSchemaChanges is the PostgreSQL wire family's spelling. Spanner
// takes it unchanged, because every table here declares a key.
func postgresSchemaChanges() schemaChanges {
	return schemaChanges{
		keyed:   "CREATE TABLE sc_pk (n int NOT NULL, PRIMARY KEY (n))",
		keyless: "CREATE TABLE sc_nopk (n int)",
		keyChange: schemaChange{
			setup: []string{
				"CREATE TABLE sc_pka (a int NOT NULL, b int NOT NULL, CONSTRAINT sc_pka_pk PRIMARY KEY (a))",
				"INSERT INTO sc_pka (a, b) VALUES (1, 1)",
			},
			before: []check{refuses("INSERT INTO sc_pka (a, b) VALUES (1, 2)")},
			// One statement, because CockroachDB refuses a table left without
			// a key between two.
			change: []string{"ALTER TABLE sc_pka DROP CONSTRAINT sc_pka_pk, ADD CONSTRAINT sc_pka_pk PRIMARY KEY (a, b)"},
			after:  []check{accepts("INSERT INTO sc_pka (a, b) VALUES (1, 2)")},
		},
		retype: schemaChange{
			setup:  []string{"CREATE TABLE sc_typ (id int NOT NULL, s varchar(4), PRIMARY KEY (id))"},
			before: []check{refuses("INSERT INTO sc_typ (id, s) VALUES (1, 'abcdefgh')")},
			change: []string{"ALTER TABLE sc_typ ALTER COLUMN s TYPE varchar(8)"},
			after:  []check{accepts("INSERT INTO sc_typ (id, s) VALUES (2, 'abcdefgh')")},
		},
		setNull: setNotNullChange(
			"CREATE TABLE sc_snn (id int NOT NULL, n int, PRIMARY KEY (id))",
			"ALTER TABLE sc_snn ALTER COLUMN n SET NOT NULL",
		),
		dropNull: dropNotNullChange(
			"CREATE TABLE sc_dnn (id int NOT NULL, n int NOT NULL, PRIMARY KEY (id))",
			"ALTER TABLE sc_dnn ALTER COLUMN n DROP NOT NULL",
		),
		defaults: defaultChange(
			"CREATE TABLE sc_def (id int NOT NULL, n int, PRIMARY KEY (id))",
			"ALTER TABLE sc_def ALTER COLUMN n SET DEFAULT 7",
			"ALTER TABLE sc_def ALTER COLUMN n DROP DEFAULT",
		),
		addColumn: addColumnChange(
			"CREATE TABLE sc_add (id int NOT NULL, PRIMARY KEY (id))",
			"ALTER TABLE sc_add ADD COLUMN n int NOT NULL DEFAULT 7",
		),
		literalDefault: "CREATE TABLE sc_exl (id int NOT NULL, s varchar(8) DEFAULT 'x', PRIMARY KEY (id))",
		exprDefault: expressionDefault(
			"CREATE TABLE sc_exe (id int NOT NULL, s varchar(8) DEFAULT (lower('X')), PRIMARY KEY (id))",
		),
		check:       "CREATE TABLE sc_chk (id int NOT NULL, n int CHECK (n > 0), PRIMARY KEY (id))",
		coverSetup:  []string{"CREATE TABLE sc_cov (id int NOT NULL, a int, b int, PRIMARY KEY (id))"},
		cover:       "CREATE INDEX sc_cov_ix ON sc_cov (a) INCLUDE (b)",
		uniqueIndex: uniqueIndexChange("CREATE TABLE sc_uix (id int NOT NULL, n int, PRIMARY KEY (id))"),
	}
}

// spannerSchemaChanges is the PostgreSQL spelling with an unnamed key, which
// is the only key Spanner takes: measured on the PGAdapter emulator v0.56.1, a
// named one answers `Setting a name of a <PRIMARY KEY> constraint is not
// supported`.
func spannerSchemaChanges() schemaChanges {
	sc := postgresSchemaChanges()
	sc.keyChange.setup = []string{
		"CREATE TABLE sc_pka (a int NOT NULL, b int NOT NULL, PRIMARY KEY (a))",
		"INSERT INTO sc_pka (a, b) VALUES (1, 1)",
	}
	sc.keyChange.change = []string{"ALTER TABLE sc_pka DROP CONSTRAINT sc_pka_pkey, ADD PRIMARY KEY (a, b)"}
	return sc
}

// mysqlSchemaChanges is the MySQL family's spelling. A length check needs a
// strict SQL mode, which both engines default to; a server running without it
// makes the type change undecidable rather than true.
func mysqlSchemaChanges() schemaChanges {
	sc := postgresSchemaChanges()
	sc.keyChange.setup = []string{
		"CREATE TABLE sc_pka (a int NOT NULL, b int NOT NULL, PRIMARY KEY (a))",
		"INSERT INTO sc_pka (a, b) VALUES (1, 1)",
	}
	sc.keyChange.change = []string{"ALTER TABLE sc_pka DROP PRIMARY KEY, ADD PRIMARY KEY (a, b)"}
	sc.retype.change = []string{"ALTER TABLE sc_typ MODIFY COLUMN s varchar(8)"}
	sc.setNull.change = []string{"ALTER TABLE sc_snn MODIFY COLUMN n int NOT NULL"}
	sc.dropNull.change = []string{"ALTER TABLE sc_dnn MODIFY COLUMN n int NULL"}
	return sc
}

// clickHouseSchemaChanges is ClickHouse's spelling. Its sorting key enforces
// no uniqueness and a NULL written into a non-nullable column becomes the
// type's default, so the evidence there is the catalog's own record of the
// column type and the key.
//
// Adding NOT NULL is asked without a DEFAULT clause, because that is the
// change the key names. Measured, 24.10 accepts it, and 26.3 and 26.9 refuse
// it with `Please specify DEFAULT expression in ALTER MODIFY COLUMN statement`.
func clickHouseSchemaChanges() schemaChanges {
	const engine = " ENGINE=MergeTree ORDER BY id"
	columnType := func(table, column, typ string) check {
		return counts(fmt.Sprintf(
			"SELECT count() FROM system.columns WHERE database = currentDatabase() "+
				"AND table = '%s' AND name = '%s' AND type = '%s'", table, column, typ), 1)
	}
	return schemaChanges{
		keyed:   "CREATE TABLE sc_pk (n Int32) ENGINE=MergeTree ORDER BY n",
		keyless: "CREATE TABLE sc_nopk (n Int32) ENGINE=MergeTree ORDER BY tuple()",
		keyChange: schemaChange{
			setup:  []string{"CREATE TABLE sc_pka (a Int32, b Int32) ENGINE=MergeTree ORDER BY a"},
			change: []string{"ALTER TABLE sc_pka MODIFY ORDER BY (a, b)"},
			after: []check{counts("SELECT count() FROM system.tables WHERE database = currentDatabase() "+
				"AND name = 'sc_pka' AND primary_key = 'a, b'", 1)},
		},
		retype: schemaChange{
			setup:  []string{"CREATE TABLE sc_typ (id Int32, n Int32)" + engine},
			change: []string{"ALTER TABLE sc_typ MODIFY COLUMN n Int64"},
			after:  []check{columnType("sc_typ", "n", "Int64")},
		},
		setNull: schemaChange{
			setup:  []string{"CREATE TABLE sc_snn (id Int32, n Nullable(Int32))" + engine},
			change: []string{"ALTER TABLE sc_snn MODIFY COLUMN n Int32"},
			after:  []check{columnType("sc_snn", "n", "Int32")},
		},
		dropNull: schemaChange{
			setup:  []string{"CREATE TABLE sc_dnn (id Int32, n Int32)" + engine},
			change: []string{"ALTER TABLE sc_dnn MODIFY COLUMN n Nullable(Int32)"},
			after:  []check{columnType("sc_dnn", "n", "Nullable(Int32)")},
		},
		defaults: defaultChange(
			"CREATE TABLE sc_def (id Int32, n Nullable(Int32))"+engine,
			"ALTER TABLE sc_def MODIFY COLUMN n DEFAULT 7",
			"ALTER TABLE sc_def MODIFY COLUMN n REMOVE DEFAULT",
		),
		addColumn: addColumnChange(
			"CREATE TABLE sc_add (id Int32)"+engine,
			"ALTER TABLE sc_add ADD COLUMN n Int32 DEFAULT 7",
		),
		literalDefault: "CREATE TABLE sc_exl (id Int32, s String DEFAULT 'x')" + engine,
		exprDefault:    expressionDefault("CREATE TABLE sc_exe (id Int32, s String DEFAULT lower('X'))" + engine),
		check:          "CREATE TABLE sc_chk (id Int32, n Int32, CONSTRAINT sc_chk_n CHECK n > 0)" + engine,
		coverSetup:     []string{"CREATE TABLE sc_cov (id Int32, a Int32, b Int32)" + engine},
		cover:          "CREATE INDEX sc_cov_ix ON sc_cov (a) INCLUDE (b)",
		uniqueIndex:    uniqueIndexChange("CREATE TABLE sc_uix (id Int32, n Int32)" + engine),
	}
}

// sqliteSchemaChanges is SQLite's spelling. SQLite does not enforce a declared
// string length, so the type change has no control before it; the ALTER it
// would need does not exist either.
func sqliteSchemaChanges() schemaChanges {
	sc := postgresSchemaChanges()
	sc.keyed = "CREATE TABLE sc_pk (n INTEGER NOT NULL, PRIMARY KEY (n))"
	sc.keyless = "CREATE TABLE sc_nopk (n INTEGER)"
	sc.keyChange.change = []string{"ALTER TABLE sc_pka ADD PRIMARY KEY (a, b)"}
	sc.retype.before = nil
	return sc
}

// sqlServerSchemaChanges is SQL Server's spelling. A default is a named
// constraint there, so setting and removing one adds and drops it.
func sqlServerSchemaChanges() schemaChanges {
	sc := postgresSchemaChanges()
	sc.keyChange.change = []string{
		"ALTER TABLE sc_pka DROP CONSTRAINT sc_pka_pk",
		"ALTER TABLE sc_pka ADD CONSTRAINT sc_pka_pk PRIMARY KEY (a, b)",
	}
	sc.retype.change = []string{"ALTER TABLE sc_typ ALTER COLUMN s varchar(8)"}
	sc.setNull.change = []string{"ALTER TABLE sc_snn ALTER COLUMN n int NOT NULL"}
	sc.dropNull.change = []string{"ALTER TABLE sc_dnn ALTER COLUMN n int NULL"}
	sc.defaults = defaultChange(
		"CREATE TABLE sc_def (id int NOT NULL, n int, PRIMARY KEY (id))",
		"ALTER TABLE sc_def ADD CONSTRAINT sc_def_n DEFAULT 7 FOR n",
		"ALTER TABLE sc_def DROP CONSTRAINT sc_def_n",
	)
	sc.addColumn.change = []string{"ALTER TABLE sc_add ADD n int NOT NULL CONSTRAINT sc_add_n DEFAULT 7"}
	sc.coverSetup, sc.cover = nil, ""
	return sc
}

// oracleSchemaChanges is Oracle's spelling.
func oracleSchemaChanges() schemaChanges {
	return schemaChanges{
		keyed:   "CREATE TABLE sc_pk (n NUMBER(10) NOT NULL, PRIMARY KEY (n))",
		keyless: "CREATE TABLE sc_nopk (n NUMBER(10))",
		keyChange: schemaChange{
			setup: []string{
				"CREATE TABLE sc_pka (a NUMBER(10) NOT NULL, b NUMBER(10) NOT NULL, CONSTRAINT sc_pka_pk PRIMARY KEY (a))",
				"INSERT INTO sc_pka (a, b) VALUES (1, 1)",
			},
			before: []check{refuses("INSERT INTO sc_pka (a, b) VALUES (1, 2)")},
			change: []string{
				"ALTER TABLE sc_pka DROP CONSTRAINT sc_pka_pk",
				"ALTER TABLE sc_pka ADD CONSTRAINT sc_pka_pk PRIMARY KEY (a, b)",
			},
			after: []check{accepts("INSERT INTO sc_pka (a, b) VALUES (1, 2)")},
		},
		retype: schemaChange{
			setup:  []string{"CREATE TABLE sc_typ (id NUMBER(10) NOT NULL, s VARCHAR2(4), PRIMARY KEY (id))"},
			before: []check{refuses("INSERT INTO sc_typ (id, s) VALUES (1, 'abcdefgh')")},
			change: []string{"ALTER TABLE sc_typ MODIFY (s VARCHAR2(8))"},
			after:  []check{accepts("INSERT INTO sc_typ (id, s) VALUES (2, 'abcdefgh')")},
		},
		setNull: setNotNullChange(
			"CREATE TABLE sc_snn (id NUMBER(10) NOT NULL, n NUMBER(10), PRIMARY KEY (id))",
			"ALTER TABLE sc_snn MODIFY (n NOT NULL)",
		),
		dropNull: dropNotNullChange(
			"CREATE TABLE sc_dnn (id NUMBER(10) NOT NULL, n NUMBER(10) NOT NULL, PRIMARY KEY (id))",
			"ALTER TABLE sc_dnn MODIFY (n NULL)",
		),
		defaults: defaultChange(
			"CREATE TABLE sc_def (id NUMBER(10) NOT NULL, n NUMBER(10), PRIMARY KEY (id))",
			"ALTER TABLE sc_def MODIFY (n DEFAULT 7)",
			"ALTER TABLE sc_def MODIFY (n DEFAULT NULL)",
		),
		addColumn: addColumnChange(
			"CREATE TABLE sc_add (id NUMBER(10) NOT NULL, PRIMARY KEY (id))",
			"ALTER TABLE sc_add ADD (n NUMBER(10) DEFAULT 7 NOT NULL)",
		),
		literalDefault: "CREATE TABLE sc_exl (id NUMBER(10) NOT NULL, s VARCHAR2(8) DEFAULT 'x', PRIMARY KEY (id))",
		exprDefault: expressionDefault(
			"CREATE TABLE sc_exe (id NUMBER(10) NOT NULL, s VARCHAR2(8) DEFAULT lower('X'), PRIMARY KEY (id))",
		),
		check:      "CREATE TABLE sc_chk (id NUMBER(10) NOT NULL, n NUMBER(10) CHECK (n > 0), PRIMARY KEY (id))",
		coverSetup: []string{"CREATE TABLE sc_cov (id NUMBER(10) NOT NULL, a NUMBER(10), b NUMBER(10), PRIMARY KEY (id))"},
		cover:      "CREATE INDEX sc_cov_ix ON sc_cov (a) INCLUDE (b)",
		uniqueIndex: uniqueIndexChange(
			"CREATE TABLE sc_uix (id NUMBER(10) NOT NULL, n NUMBER(10), PRIMARY KEY (id))",
		),
	}
}

// ydbSchemaChanges is YDB's spelling. Every key is declared in its own clause,
// an index is added through ALTER TABLE, and a default is a typed literal.
//
// Three experiments differ in shape from the PostgreSQL ones, each for a
// measured reason. YDB has no length on a string type, so the type change is
// asked of an integer, and a value only Int64 holds is the evidence: an Int32
// column refuses 5000000000 as a type mismatch. YDB folds a constant
// expression into a literal at CREATE TABLE from 26.1 (`DEFAULT 1 + 1` and
// `DEFAULT Unicode::ToLower("X"u)` are accepted there and refused on 25.x), so
// a deterministic default cannot tell an evaluated expression from a folded
// literal; the expression is the per-row function the key's own documentation
// names, `CurrentUtcTimestamp()`, which every line refuses with `Unsupported
// type of literal`. And no SQL describes an index, so the covering index is
// read back through Ptah's own reader.
func ydbSchemaChanges() schemaChanges {
	table := func(name, columns string) string {
		return fmt.Sprintf("CREATE TABLE %s (%s, PRIMARY KEY (id))", name, columns)
	}
	return schemaChanges{
		keyed:   "CREATE TABLE sc_pk (n Int32 NOT NULL, PRIMARY KEY (n))",
		keyless: "CREATE TABLE sc_nopk (n Int32)",
		keyChange: schemaChange{
			setup: []string{
				"CREATE TABLE sc_pka (a Int32 NOT NULL, b Int32 NOT NULL, PRIMARY KEY (a))",
				"INSERT INTO sc_pka (a, b) VALUES (1, 1)",
			},
			before: []check{refuses("INSERT INTO sc_pka (a, b) VALUES (1, 2)")},
			change: []string{"ALTER TABLE sc_pka DROP PRIMARY KEY, ADD PRIMARY KEY (a, b)"},
			after:  []check{accepts("INSERT INTO sc_pka (a, b) VALUES (1, 2)")},
		},
		retype: schemaChange{
			setup:  []string{table("sc_typ", "id Int32 NOT NULL, n Int32")},
			before: []check{refuses("INSERT INTO sc_typ (id, n) VALUES (1, 5000000000)")},
			change: []string{"ALTER TABLE sc_typ ALTER COLUMN n SET DATA TYPE Int64"},
			after:  []check{accepts("INSERT INTO sc_typ (id, n) VALUES (2, 5000000000)")},
		},
		setNull: setNotNullChange(table("sc_snn", "id Int32 NOT NULL, n Int32"),
			"ALTER TABLE sc_snn ALTER COLUMN n SET NOT NULL"),
		dropNull: dropNotNullChange(table("sc_dnn", "id Int32 NOT NULL, n Int32 NOT NULL"),
			"ALTER TABLE sc_dnn ALTER COLUMN n DROP NOT NULL"),
		defaults: defaultChange(table("sc_def", "id Int32 NOT NULL, n Int32"),
			"ALTER TABLE sc_def ALTER COLUMN n SET DEFAULT 7",
			"ALTER TABLE sc_def ALTER COLUMN n DROP DEFAULT"),
		addColumn: addColumnChange(table("sc_add", "id Int32 NOT NULL"),
			"ALTER TABLE sc_add ADD COLUMN n Int32 NOT NULL DEFAULT 7"),
		literalDefault: table("sc_exl", `id Int32 NOT NULL, t Timestamp DEFAULT Timestamp("2026-01-01T00:00:00Z")`),
		exprDefault: schemaChange{
			change: []string{table("sc_exe", "id Int32 NOT NULL, t Timestamp DEFAULT CurrentUtcTimestamp()")},
			after: []check{
				accepts("INSERT INTO sc_exe (id) VALUES (1)"),
				counts("SELECT COUNT(*) FROM sc_exe WHERE id = 1 AND t IS NOT NULL", 1),
			},
		},
		check:      table("sc_chk", "id Int32 NOT NULL, n Int32 CHECK (n > 0)"),
		coverSetup: []string{table("sc_cov", "id Int32 NOT NULL, a Int32, b Int32")},
		cover:      "ALTER TABLE sc_cov ADD INDEX sc_cov_ix GLOBAL ON (a) COVER (b)",
		coverRead: []check{ydbDescribedIndex("sc_cov", "sc_cov_ix", "the index to cover b",
			func(index catalog.Index) bool { return slices.Equal(index.IncludeColumns, []string{"b"}) })},
		uniqueIndex: schemaChange{
			setup:  []string{table("sc_uix", "id Int32 NOT NULL, n Int32"), "INSERT INTO sc_uix (id, n) VALUES (1, 10)"},
			change: []string{"ALTER TABLE sc_uix ADD INDEX sc_uix_n GLOBAL UNIQUE SYNC ON (n)"},
			after: []check{
				accepts("INSERT INTO sc_uix (id, n) VALUES (2, 20)"),
				refuses("INSERT INTO sc_uix (id, n) VALUES (3, 10)"),
			},
		},
	}
}

// setNotNullChange adds NOT NULL to a column that accepted a NULL a moment
// earlier. The NULL row is removed again before the change, because a column
// holding one cannot take the constraint on any engine.
func setNotNullChange(table, change string) schemaChange {
	return schemaChange{
		setup: []string{table},
		before: []check{
			accepts("INSERT INTO sc_snn (id, n) VALUES (1, NULL)"),
			accepts("DELETE FROM sc_snn WHERE id = 1"),
		},
		change: []string{change},
		after: []check{
			accepts("INSERT INTO sc_snn (id, n) VALUES (2, 2)"),
			refuses("INSERT INTO sc_snn (id, n) VALUES (3, NULL)"),
		},
	}
}

// dropNotNullChange removes NOT NULL from a column that refused a NULL before.
func dropNotNullChange(table, change string) schemaChange {
	return schemaChange{
		setup:  []string{table},
		before: []check{refuses("INSERT INTO sc_dnn (id, n) VALUES (1, NULL)")},
		change: []string{change},
		after:  []check{accepts("INSERT INTO sc_dnn (id, n) VALUES (2, NULL)")},
	}
}

// defaultChange sets a default, proves a row takes it, removes it, and proves
// the next row does not.
//
// The second row may be refused rather than written with a NULL: measured on
// MySQL 8.4, DROP DEFAULT leaves a nullable column with no default at all, and
// a strict session answers `Field 'n' doesn't have a default value`. Either
// way the removed default no longer fills the row, which is what the count
// after it asks.
func defaultChange(table, set, drop string) schemaChange {
	return schemaChange{
		setup:  []string{table},
		change: []string{set},
		after: []check{
			accepts("INSERT INTO sc_def (id) VALUES (1)"),
			counts("SELECT COUNT(*) FROM sc_def WHERE id = 1 AND n = 7", 1),
			accepts(drop),
			attempts("INSERT INTO sc_def (id) VALUES (2)"),
			counts("SELECT COUNT(*) FROM sc_def WHERE id = 2 AND n = 7", 0),
		},
	}
}

// addColumnChange adds a column with a default to a table holding a row, and
// reads that row back.
func addColumnChange(table, change string) schemaChange {
	return schemaChange{
		setup:  []string{table, "INSERT INTO sc_add (id) VALUES (1)"},
		change: []string{change},
		after:  []check{counts("SELECT COUNT(*) FROM sc_add WHERE id = 1 AND n = 7", 1)},
	}
}

// expressionDefault creates a table whose default is a function call and
// proves a row receives its value.
func expressionDefault(table string) schemaChange {
	return schemaChange{
		change: []string{table},
		after: []check{
			accepts("INSERT INTO sc_exe (id) VALUES (1)"),
			counts("SELECT COUNT(*) FROM sc_exe WHERE id = 1 AND s = 'x'", 1),
		},
	}
}

// uniqueIndexChange adds a unique index to a table that holds a row, and
// proves it refuses a duplicate while taking a new value.
func uniqueIndexChange(table string) schemaChange {
	return schemaChange{
		setup:  []string{table, "INSERT INTO sc_uix (id, n) VALUES (1, 10)"},
		change: []string{"CREATE UNIQUE INDEX sc_uix_n ON sc_uix (n)"},
		after: []check{
			accepts("INSERT INTO sc_uix (id, n) VALUES (2, 20)"),
			refuses("INSERT INTO sc_uix (id, n) VALUES (3, 10)"),
		},
	}
}
