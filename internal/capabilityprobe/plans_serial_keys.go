package capabilityprobe

import (
	"context"
	"fmt"
	"path"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/dbschema"
	"ptah.run/internal/ydbsequence"
)

// withSerialKeys adds the questions the YDB planner decides a Serial column's
// sequence with: whether Ptah changes its start and increment, and whether a
// 16-bit or 32-bit Serial's sequence keeps ending where its column does
// through that change.
//
// On YDB each is asked in YDB's spelling, ALTER SEQUENCE on the sequence's
// absolute path, and then used: rows are written and counted, and the settings
// are read back through Ptah's reader. Elsewhere the first key names whether
// Ptah's planner compares and plans a serial's sequence, which only the YDB
// planner does, and the second presupposes it, so both are declared rather
// than asked: what another engine's ALTER SEQUENCE does says nothing about
// what that dialect's planner plans.
func withSerialKeys(p plan, dialect string) plan {
	if platform.NormalizeDialect(dialect) == platform.YDB {
		p.experiments = append(p.experiments, ydbSerialKeyExperiments()...)
		return p
	}
	if p.undecided == nil {
		p.undecided = make(map[capability.Capability]string)
	}
	p.undecided[capability.SerialSequenceOptions] = "the key names whether Ptah's planner compares a serial " +
		"column's sequence start and increment and plans ALTER SEQUENCE for a change, which only the YDB planner " +
		"does; this server altering a sequence or not says nothing about what this dialect's planner plans"
	p.undecided[capability.SerialSequenceKeepsRange] = "the key says whether Ptah may change a 16-bit or " +
		"32-bit Serial's sequence, and presupposes serial_sequence_options, which only the YDB planner has"
	return p
}

// ydbSerialKeyExperiments asks YDB to give a BigSerial's sequence a start and
// an increment, and to restart a SmallSerial's sequence at the column's
// maximum, and uses each.
//
// The restart is the cheapest way to reach the end of a SmallSerial: before
// any ALTER, the sequence ends at 32767 and refuses a value after it, and the
// question is whether it still does after one. A line that keeps the range
// refuses the second row; a line that widens it stores that row as -32768.
func ydbSerialKeyExperiments() []experiment {
	t := ydbSpelling
	return []experiment{
		ydbSequenceExperiment(capability.SerialSequenceOptions, nil,
			t.table("sso", "id BigSerial NOT NULL, n Int64", "id"), "sso", "id",
			"START WITH 100 INCREMENT BY 5 RESTART",
			accepts("INSERT INTO sso (n) VALUES (1)"),
			accepts("INSERT INTO sso (n) VALUES (2)"),
			counts("SELECT COUNT(*) FROM sso WHERE id = 100 OR id = 105", 2),
			ydbDescribedColumn("sso", "id", "the sequence of id to read back as starting at 100 and stepping by 5",
				func(column catalog.Column) bool {
					return column.IsAutoIncrement && column.IdentityStart == "100" && column.IdentityIncrement == "5"
				}),
		),
		ydbSequenceExperiment(capability.SerialSequenceKeepsRange,
			[]capability.Capability{capability.SerialSequenceOptions},
			t.table("skr", "id SmallSerial NOT NULL, n Int64", "id"), "skr", "id",
			"RESTART WITH 32767",
			accepts("INSERT INTO skr (n) VALUES (1)"),
			attempts("INSERT INTO skr (n) VALUES (2)"),
			// The cast keeps an Int16 literal out of the query: 25.1.4.7
			// answers one with `FillLiteralProtoImpl(): requirement false
			// failed`, which would decide the key on the line's literal bug.
			counts("SELECT COUNT(*) FROM skr WHERE CAST(id AS Int64) < 0", 0),
		),
	}
}

// ydbSequenceExperiment decides key by creating table with setup, altering the
// sequence of its Serial column with clause, and running after. The ALTER is
// built at run time, because YDB takes the sequence only by its absolute path,
// which names the database and the probe's directory.
//
// An ALTER the server refuses decides the key false; one it accepts decides it
// true when every check after it holds, and false with the failed check as the
// note otherwise.
func ydbSequenceExperiment(
	key capability.Capability,
	requires []capability.Capability,
	setup, table, column, clause string,
	after ...check,
) experiment {
	return experiment{
		decides:  []capability.Capability{key},
		requires: requires,
		setup:    []string{setup},
		decide: func(ctx context.Context, s *session) (verdicts, []Attempt) {
			sequence := ydbsequence.Path(s.database, s.namespace, table, column)
			altered := s.exec(ctx, fmt.Sprintf("ALTER SEQUENCE `%s` %s", sequence, clause))
			attempts := []Attempt{altered}
			if !altered.Accepted {
				return verdicts{key: decided(false)}, attempts
			}
			for _, evidence := range after {
				attempt, held, did := evidence.run(ctx, s)
				attempts = append(attempts, attempt)
				if !held {
					return verdicts{key: annotated(false,
						"the ALTER SEQUENCE was accepted, and then expected "+evidence.expectation()+", and it "+did,
					)}, attempts
				}
			}
			return verdicts{key: decided(true)}, attempts
		},
	}
}

// ydbDescribedColumn is a check that reads a column back through Ptah's own
// YDB reader, scoped to the probe's directory, and holds when want answers
// true for what it read. A sequence's settings are in the table's description
// and in no SQL a query can select, so the reader is the read.
func ydbDescribedColumn(table, column, expectation string, want func(catalog.Column) bool) check {
	return check{
		describes: expectation,
		inspect: func(ctx context.Context, s *session) (Attempt, bool, string) {
			attempt := Attempt{Statement: fmt.Sprintf("read column %s of table %s through Ptah's YDB reader",
				column, path.Join(s.database, s.namespace, table))}
			db, err := dbschema.ReadSchemaWithSchemasContext(ctx, s.conn, []string{s.namespace})
			if err != nil {
				attempt.ServerErr = err.Error()
				return attempt, false, "was refused"
			}
			attempt.Accepted = true
			for _, found := range db.Tables {
				if found.Name != table {
					continue
				}
				for _, described := range found.Columns {
					if described.Name == column {
						return attempt, want(described), fmt.Sprintf("read start %q and increment %q",
							described.IdentityStart, described.IdentityIncrement)
					}
				}
			}
			return attempt, false, "found no such column"
		},
	}
}
