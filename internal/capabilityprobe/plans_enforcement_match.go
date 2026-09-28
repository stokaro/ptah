package capabilityprobe

import (
	"context"
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
)

// withEnforcementAndMatch adds the questions whether a CHECK and a foreign key
// can be declared NOT ENFORCED, and whether a foreign key keeps MATCH FULL
// and MATCH PARTIAL (stokaro/ptah#3853).
//
// Every dialect is asked, so a refusal is a measured answer. The MATCH keys
// are decided by what the server records rather than by what it accepts:
// MariaDB 11.8.9 and SQLite 3.51 accept the clause and record NONE, and
// acceptance alone would credit them with a key they drop. Each experiment
// creates tables of its own, because a setup that fails makes its keys
// undecidable.
func withEnforcementAndMatch(p plan, dialect string) plan {
	if dialect == platform.SQLite {
		p.experiments = append(p.experiments, sqliteEnforcementAndMatch()...)
		return p
	}
	tables := enforcementTables(dialect)
	p.experiments = append(p.experiments,
		acceptance(capability.NotEnforcedChecks, []string{tables.child("enc")},
			"ALTER TABLE enc ADD CONSTRAINT enc_ck CHECK (n > 0) NOT ENFORCED"),
		acceptance(capability.NotEnforcedForeignKeys, []string{tables.parent("emp1"), tables.child("emf1")},
			"ALTER TABLE emf1 ADD CONSTRAINT emf1_fk FOREIGN KEY (a) REFERENCES emp1 (id) NOT ENFORCED"),
		matchExperiment(capability.ForeignKeyMatchFull, dialect, tables, "FULL", "emp2", "emf2"),
		matchExperiment(capability.ForeignKeyMatchPartial, dialect, tables, "PARTIAL", "emp3", "emf3"),
	)
	return p
}

// enforcementTableSpelling writes the throwaway tables the experiments take:
// a referenced table keyed on id, and a referencing table with n and a.
type enforcementTableSpelling struct {
	parentColumns string
	childColumns  string
	spelling      tableSpelling
}

func enforcementTables(dialect string) enforcementTableSpelling {
	switch dialect {
	case platform.Postgres, platform.CockroachDB, platform.YugabyteDB, platform.Spanner:
		spelling := postgresFamilySpelling(dialect)
		parent := "id int PRIMARY KEY"
		if spelling.keyed {
			parent = "id int"
		}
		return enforcementTableSpelling{parentColumns: parent, childColumns: "n int, a int", spelling: spelling}
	case platform.ClickHouse:
		return enforcementTableSpelling{
			parentColumns: "id Int64", childColumns: "n Int64, a Int64", spelling: clickHouseSpelling,
		}
	case platform.Oracle:
		return enforcementTableSpelling{
			parentColumns: "id NUMBER(10) PRIMARY KEY", childColumns: "n NUMBER(10), a NUMBER(10)",
		}
	default:
		return enforcementTableSpelling{parentColumns: "id int PRIMARY KEY", childColumns: "n int, a int"}
	}
}

func (t enforcementTableSpelling) parent(name string) string {
	return t.spelling.table(name, t.parentColumns, "id")
}

func (t enforcementTableSpelling) child(name string) string {
	return t.spelling.table(name, t.childColumns, "n")
}

// matchExperiment asks whether the server keeps a foreign key's MATCH type:
// that it accepts the key, and that the catalog the reader reads records the
// type. A dialect with no catalog read here is decided by acceptance, which
// every such dialect refuses.
func matchExperiment(
	key capability.Capability, dialect string, tables enforcementTableSpelling, match, parent, child string,
) experiment {
	setup := []string{tables.parent(parent), tables.child(child)}
	statement := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s_fk FOREIGN KEY (a) REFERENCES %s (id) MATCH %s",
		child, child, parent, match)
	switch dialect {
	case platform.Postgres, platform.CockroachDB, platform.YugabyteDB, platform.Spanner:
		return recordedExperiment(key, setup, statement, fmt.Sprintf(
			"SELECT COUNT(*) FROM pg_constraint WHERE conname = '%s_fk' AND confmatchtype = '%s'",
			child, postgresMatchType[match]))
	case platform.MySQL, platform.MariaDB:
		return recordedExperiment(key, setup, statement, fmt.Sprintf(
			"SELECT COUNT(*) FROM information_schema.REFERENTIAL_CONSTRAINTS "+
				"WHERE CONSTRAINT_SCHEMA = DATABASE() AND CONSTRAINT_NAME = '%s_fk' AND MATCH_OPTION = '%s'",
			child, match))
	default:
		return acceptance(key, setup, statement)
	}
}

// postgresMatchType is how pg_constraint.confmatchtype spells a MATCH type.
var postgresMatchType = map[string]string{"FULL": "f", "PARTIAL": "p"}

// sqliteEnforcementAndMatch asks SQLite the same questions in CREATE TABLE,
// because it adds no constraint to a table that exists.
func sqliteEnforcementAndMatch() []experiment {
	parent := func(name string) string { return "CREATE TABLE " + name + " (id INTEGER PRIMARY KEY)" }
	sqliteMatch := func(key capability.Capability, match, parentName, child string) experiment {
		return recordedExperiment(key, []string{parent(parentName)},
			fmt.Sprintf("CREATE TABLE %s (a INTEGER, CONSTRAINT %s_fk FOREIGN KEY (a) REFERENCES %s (id) MATCH %s)",
				child, child, parentName, match),
			fmt.Sprintf(`SELECT COUNT(*) FROM pragma_foreign_key_list('%s') WHERE "match" = '%s'`, child, match))
	}
	return []experiment{
		acceptance(capability.NotEnforcedChecks, nil,
			"CREATE TABLE enc (n INTEGER, CONSTRAINT enc_ck CHECK (n > 0) NOT ENFORCED)"),
		acceptance(capability.NotEnforcedForeignKeys, []string{parent("emp1")},
			"CREATE TABLE emf1 (a INTEGER, CONSTRAINT emf1_fk FOREIGN KEY (a) REFERENCES emp1 (id) NOT ENFORCED)"),
		sqliteMatch(capability.ForeignKeyMatchFull, "FULL", "emp2", "emf2"),
		sqliteMatch(capability.ForeignKeyMatchPartial, "PARTIAL", "emp3", "emf3"),
	}
}

// recordedExperiment decides key by whether the server accepts statement and
// then reports what it declares in readBack, which counts the rows that
// record it: one row is a key the server keeps, and none is a clause the
// server took and dropped.
func recordedExperiment(key capability.Capability, setup []string, statement, readBack string) experiment {
	return experiment{
		decides: []capability.Capability{key},
		setup:   setup,
		decide: func(ctx context.Context, s *session) (verdicts, []Attempt) {
			attempt := s.exec(ctx, statement)
			if !attempt.Accepted {
				return verdicts{key: decided(false)}, []Attempt{attempt}
			}
			stored, read := s.query(ctx, readBack)
			return verdicts{key: recordedObservation(read, stored)}, []Attempt{attempt, read}
		},
	}
}

// recordedObservation reads one recorded-clause answer.
func recordedObservation(read Attempt, stored int64) observation {
	if !read.Accepted {
		return cannotDecide("the clause was accepted and the read-back %q was refused (%s)",
			collapse(read.Statement), collapse(read.ServerErr))
	}
	if stored != 1 {
		return annotated(false, "the server accepted the clause and does not record it")
	}
	return decided(true)
}
