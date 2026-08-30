//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/dbtarget"
)

type observedIncludeIndex struct {
	Name           string
	Method         string
	IncludeColumns []string
}

// A covering index must come back carrying both the payload and an access
// method the server that produced it will accept.
//
// Two separate defects made this fail on CockroachDB, and each hid behind the
// other (stokaro/ptah#2584). The renderer refused INCLUDE for the dialect
// outright, so a database holding one could not even be described; behind that,
// pg_am reports `prefix` for every b-tree index there and CockroachDB rejects
// that name as input, so the description replayed as
// `unrecognized access method: prefix`.
//
// PostgreSQL and YugabyteDB are here as controls rather than as coverage: they
// report `btree` themselves, so a rewrite that fired on every dialect instead of
// on CockroachDB alone still passes the CockroachDB row and fails these.
func TestReaderIndexInclude_LiveCarriesPayloadAndAcceptedAccessMethod(t *testing.T) {
	tests := []struct {
		name   string
		engine dbtarget.Engine
		want   []observedIncludeIndex
	}{
		{
			name:   "PostgreSQL",
			engine: dbtarget.PostgreSQL,
			want:   expectedIncludeIndexes(),
		},
		{
			name:   "CockroachDB",
			engine: dbtarget.CockroachDB,
			want:   expectedIncludeIndexes(),
		},
		{
			name:   "YugabyteDB",
			engine: dbtarget.YugabyteDB,
			want:   expectedIncludeIndexes(),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(c.Context(), time.Minute)
			defer cancel()

			conn, schemaName := prepareIncludeIndexFixture(c, ctx, test.engine)
			gotSchema, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{schemaName})

			c.Assert(err, qt.IsNil)
			c.Assert(observeIncludeIndexes(gotSchema.Indexes), qt.DeepEquals, test.want)
		})
	}
}

// The description the reader produced must replay. Asserting the model alone
// would still pass with an access-method name the server refuses, because
// nothing in the model knows which names are input and which are catalog-only.
func TestReaderIndexInclude_LiveDescriptionReplays(t *testing.T) {
	tests := []struct {
		name   string
		engine dbtarget.Engine
	}{
		{name: "PostgreSQL", engine: dbtarget.PostgreSQL},
		{name: "CockroachDB", engine: dbtarget.CockroachDB},
		{name: "YugabyteDB", engine: dbtarget.YugabyteDB},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(c.Context(), 2*time.Minute)
			defer cancel()

			conn, schemaName := prepareIncludeIndexFixture(c, ctx, test.engine)
			gotSchema, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{schemaName})
			c.Assert(err, qt.IsNil)

			replaySchema := fmt.Sprintf("%s_replay", schemaName)
			replayIdent := pgx.Identifier{replaySchema}.Sanitize()
			c.Cleanup(func() {
				dropIncludeIndexFixture(c, context.Background(), conn, replayIdent)
			})
			_, err = conn.ExecContext(ctx, fmt.Sprintf("CREATE SCHEMA %s", replayIdent))
			c.Assert(err, qt.IsNil)
			_, err = conn.ExecContext(ctx, includeIndexTableSQL(replayIdent))
			c.Assert(err, qt.IsNil)

			for _, index := range observeIncludeIndexes(gotSchema.Indexes) {
				statement := fmt.Sprintf(
					"CREATE INDEX %s ON %s.covering USING %s (email) INCLUDE (%s)",
					pgx.Identifier{index.Name + "_replay"}.Sanitize(),
					replayIdent,
					index.Method,
					index.IncludeColumns[0],
				)
				_, err = conn.ExecContext(ctx, statement)
				c.Assert(err, qt.IsNil, qt.Commentf("replay index description: %s", statement))
			}
		})
	}
}

func expectedIncludeIndexes() []observedIncludeIndex {
	return []observedIncludeIndex{
		{Name: "idx_covering_email", Method: "btree", IncludeColumns: []string{"display_name"}},
	}
}

func prepareIncludeIndexFixture(
	c *qt.C,
	ctx context.Context,
	engine dbtarget.Engine,
) (*dbschema.DatabaseConnection, string) {
	c.Helper()
	rawURL := dbtarget.URL(c, engine)
	conn, err := dbschema.ConnectToDatabase(ctx, rawURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Check(conn.Close(), qt.IsNil)
	})

	schemaName := fmt.Sprintf("ptah_index_include_%d", time.Now().UnixNano())
	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	dropIncludeIndexFixture(c, context.Background(), conn, schemaIdent)
	c.Cleanup(func() {
		dropIncludeIndexFixture(c, context.Background(), conn, schemaIdent)
	})

	statements := []string{
		fmt.Sprintf("CREATE SCHEMA %s", schemaIdent),
		includeIndexTableSQL(schemaIdent),
		fmt.Sprintf(
			"CREATE INDEX idx_covering_email ON %s.covering (email) INCLUDE (display_name)",
			schemaIdent,
		),
	}
	for _, statement := range statements {
		_, err := conn.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("execute include index fixture statement: %s", statement))
	}
	return conn, schemaName
}

func includeIndexTableSQL(schemaIdent string) string {
	return fmt.Sprintf(`CREATE TABLE %s.covering (
		id bigint NOT NULL,
		email text NOT NULL,
		display_name text,
		CONSTRAINT pk_covering PRIMARY KEY (id)
	)`, schemaIdent)
}

func dropIncludeIndexFixture(
	c *qt.C,
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	schemaIdent string,
) {
	c.Helper()
	_, err := conn.ExecContext(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schemaIdent))
	c.Check(err, qt.IsNil)
}

func observeIncludeIndexes(indexes []catalog.Index) []observedIncludeIndex {
	observed := make([]observedIncludeIndex, 0)
	for _, index := range indexes {
		if len(index.IncludeColumns) == 0 {
			continue
		}
		observed = append(observed, observedIncludeIndex{
			Name:           index.Name,
			Method:         index.Method,
			IncludeColumns: slices.Clone(index.IncludeColumns),
		})
	}
	slices.SortFunc(observed, func(a, b observedIncludeIndex) int {
		return slices.Compare([]string{a.Name}, []string{b.Name})
	})
	return observed
}
