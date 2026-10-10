package capabilityprobe

import (
	"context"
	"fmt"
	"path"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/dbschema"
	"ptah.run/dialect/ydb/ydbschema"
)

// withColumnFamilyKeys adds the questions the YDB renderer, reader and planner
// decide a row table's column families with: whether a table carries families
// at all, and whether the line takes a family's cache mode.
//
// On YDB each is asked in YDB's spelling, `ALTER TABLE ... ADD FAMILY`, and the
// family is then read back through Ptah's reader, since no SQL describes one.
// A pool kind differs from cluster to cluster, so no question names DATA. A
// column family in this sense is YDB's, so every other engine is asked the
// same statement and its refusal is the measurement, as with a changefeed;
// CockroachDB's FAMILY clause groups columns with no settings, and Ptah models
// none.
func withColumnFamilyKeys(p plan, dialect string) plan {
	normalized := platform.NormalizeDialect(dialect)
	if normalized == platform.YDB {
		p.experiments = append(p.experiments, ydbColumnFamilyExperiments()...)
		return p
	}
	spelling, ok := typeKeySpellingFor(normalized)
	if !ok {
		return p
	}
	for _, question := range columnFamilyQuestions() {
		p.experiments = append(p.experiments, proven(question.key, schemaChange{
			setup:  []string{spelling.table(question.table, "n "+spelling.integer)},
			change: []string{question.statement},
		}))
	}
	return p
}

// columnFamilyQuestion is one statement the probe sends about column
// families, on the table it creates for the purpose.
type columnFamilyQuestion struct {
	key       capability.Capability
	table     string
	statement string
}

// columnFamilyQuestions are the statements each key is asked with, the same on
// every engine.
func columnFamilyQuestions() []columnFamilyQuestion {
	return []columnFamilyQuestion{
		{key: capability.ColumnFamilies, table: "cff",
			statement: "ALTER TABLE cff ADD FAMILY cold (COMPRESSION = 'lz4'), ALTER COLUMN n SET FAMILY cold"},
		{key: capability.ColumnFamilyCacheMode, table: "cfm",
			statement: "ALTER TABLE cfm ADD FAMILY hot (CACHE_MODE = 'in_memory')"},
	}
}

// ydbColumnFamilyExperiments sends each question to YDB and reads the family
// back.
func ydbColumnFamilyExperiments() []experiment {
	t := ydbSpelling
	wants := map[capability.Capability]struct {
		expectation string
		want        func(ydbschema.ColumnFamily) bool
	}{
		capability.ColumnFamilies: {
			expectation: "family cold to read back compressed with lz4 and holding column n",
			want: func(family ydbschema.ColumnFamily) bool {
				return family.Name == "cold" && family.Compression == "lz4" &&
					len(family.Columns) == 1 && family.Columns[0] == "n"
			},
		},
		capability.ColumnFamilyCacheMode: {
			expectation: "family hot to read back kept in memory",
			want: func(family ydbschema.ColumnFamily) bool {
				return family.Name == "hot" && family.CacheMode == "in_memory"
			},
		},
	}
	questions := columnFamilyQuestions()
	experiments := make([]experiment, 0, len(questions))
	for _, question := range questions {
		read := wants[question.key]
		experiments = append(experiments, proven(question.key, schemaChange{
			setup:  []string{t.table(question.table, "id Uint64 NOT NULL, n Int64", "id")},
			change: []string{question.statement},
			after:  []check{ydbDescribedFamilies(question.table, read.expectation, read.want)},
		}))
	}
	return experiments
}

// ydbDescribedFamilies reads table through Ptah's YDB reader and holds when
// one of its column families reads back as want says.
func ydbDescribedFamilies(table, expectation string, want func(ydbschema.ColumnFamily) bool) check {
	return check{
		describes: expectation,
		inspect: func(ctx context.Context, s *session) (Attempt, bool, string) {
			attempt := Attempt{Statement: fmt.Sprintf("read the column families of table %s through Ptah's YDB reader",
				path.Join(s.database, s.namespace, table))}
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
				observed, _, err := schemaext.FacetAs[*ydbschema.ObservedColumnFamilies](found.Facets, ydbschema.ColumnFamiliesKind)
				if err != nil || observed == nil {
					return attempt, false, fmt.Sprintf("read no column families (%v)", err)
				}
				for _, family := range observed.Families {
					if want(family) {
						return attempt, true, fmt.Sprintf("read %+v", family)
					}
				}
				return attempt, false, fmt.Sprintf("read %+v", observed.Families)
			}
			return attempt, false, "found no such table"
		},
	}
}
