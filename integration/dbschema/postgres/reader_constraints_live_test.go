//go:build integration

package postgres_test

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5"

	"go.5x5.cz/ptah/dbschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/dbtarget"
)

type observedConstraint struct {
	Table          string
	Name           string
	Type           string
	Columns        []string
	ForeignTable   string
	ForeignColumns []string
	OnDelete       string
	OnUpdate       string
	HasCheckClause bool
}

func TestReaderConstraints_LiveKeepsSameNamedConstraintsTableQualified(t *testing.T) {
	tests := []struct {
		name   string
		engine dbtarget.Engine
		want   []observedConstraint
	}{
		{
			name:   "PostgreSQL",
			engine: dbtarget.PostgreSQL,
			want:   expectedConstraints([]string{"amount"}),
		},
		{
			name:   "CockroachDB",
			engine: dbtarget.CockroachDB,
			want:   expectedConstraints([]string{"amount"}),
		},
		{
			name:   "YugabyteDB",
			engine: dbtarget.YugabyteDB,
			want:   expectedConstraints([]string{"amount"}),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(c.Context(), time.Minute)
			defer cancel()
			conn, schemaName := prepareConstraintIdentityFixture(c, ctx, test.engine)
			gotSchema, err := dbschema.ReadSchemaWithSchemas(conn, []string{schemaName})
			c.Assert(err, qt.IsNil)
			c.Assert(observeConstraints(gotSchema.Constraints), qt.DeepEquals, test.want)
		})
	}
}

func prepareConstraintIdentityFixture(c *qt.C, ctx context.Context, engine dbtarget.Engine) (*dbschema.DatabaseConnection, string) {
	c.Helper()
	rawURL := dbtarget.URL(c, engine)
	conn, err := dbschema.ConnectToDatabase(ctx, rawURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Check(conn.Close(), qt.IsNil)
	})

	schemaName := fmt.Sprintf("ptah_constraint_identity_%d", time.Now().UnixNano())
	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	dropConstraintIdentityFixture(c, context.Background(), conn, schemaIdent)
	c.Cleanup(func() {
		dropConstraintIdentityFixture(c, context.Background(), conn, schemaIdent)
	})

	statements := []string{
		fmt.Sprintf("CREATE SCHEMA %s", schemaIdent),
		fmt.Sprintf(`CREATE TABLE %s.tenants (
			id bigint,
			region text,
			PRIMARY KEY (id, region)
		)`, schemaIdent),
		constraintIdentityChildTableSQL(schemaIdent, "locations", "NO ACTION"),
		constraintIdentityChildTableSQL(schemaIdent, "areas", "CASCADE"),
		constraintIdentityChildTableSQL(schemaIdent, "commodities", "SET NULL"),
	}
	for _, statement := range statements {
		_, err := conn.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("execute constraint identity fixture statement: %s", statement))
	}
	return conn, schemaName
}

func constraintIdentityChildTableSQL(schemaIdent, table, deleteRule string) string {
	return fmt.Sprintf(`CREATE TABLE %s.%s (
		id bigint NOT NULL,
		code text NOT NULL,
		amount bigint NOT NULL,
		tenant_id bigint,
		tenant_region text,
		CONSTRAINT pk_%[2]s PRIMARY KEY (id),
		CONSTRAINT uq_%[2]s_code UNIQUE (code),
		CONSTRAINT ck_entity_amount CHECK (amount >= 0),
		CONSTRAINT fk_entity_tenant FOREIGN KEY (tenant_id, tenant_region)
			REFERENCES %[1]s.tenants(id, region) ON DELETE %[3]s
	)`, schemaIdent, table, deleteRule)
}

func dropConstraintIdentityFixture(
	c *qt.C,
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	schemaIdent string,
) {
	c.Helper()
	_, err := conn.ExecContext(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schemaIdent))
	c.Check(err, qt.IsNil)
}

func observeConstraints(constraints []dbschematypes.DBConstraint) []observedConstraint {
	observed := make([]observedConstraint, 0)
	for _, constraint := range constraints {
		if !slices.Contains([]string{"ck_entity_amount", "fk_entity_tenant"}, constraint.Name) &&
			!strings.HasPrefix(constraint.Name, "pk_") &&
			!strings.HasPrefix(constraint.Name, "uq_") {
			continue
		}
		observed = append(observed, observedConstraint{
			Table:          constraint.TableName,
			Name:           constraint.Name,
			Type:           constraint.Type,
			Columns:        slices.Clone(constraint.ColumnNames),
			ForeignTable:   dereferenceString(constraint.ForeignTable),
			ForeignColumns: slices.Clone(constraint.ForeignColumns),
			OnDelete:       dereferenceString(constraint.DeleteRule),
			OnUpdate:       dereferenceString(constraint.UpdateRule),
			HasCheckClause: hasText(constraint.CheckClause),
		})
	}
	slices.SortFunc(observed, func(left, right observedConstraint) int {
		return cmp.Or(
			cmp.Compare(left.Table, right.Table),
			cmp.Compare(left.Type, right.Type),
		)
	})
	return observed
}

func expectedConstraints(checkColumns []string) []observedConstraint {
	constraints := make([]observedConstraint, 0, 12)
	for _, table := range []struct {
		name       string
		deleteRule string
	}{
		{name: "areas", deleteRule: "CASCADE"},
		{name: "commodities", deleteRule: "SET NULL"},
		{name: "locations", deleteRule: "NO ACTION"},
	} {
		constraints = append(constraints,
			observedConstraint{
				Table:          table.name,
				Name:           "ck_entity_amount",
				Type:           "CHECK",
				Columns:        slices.Clone(checkColumns),
				HasCheckClause: true,
			},
			observedConstraint{
				Table:          table.name,
				Name:           "fk_entity_tenant",
				Type:           "FOREIGN KEY",
				Columns:        []string{"tenant_id", "tenant_region"},
				ForeignTable:   "tenants",
				ForeignColumns: []string{"id", "region"},
				OnDelete:       table.deleteRule,
				OnUpdate:       "NO ACTION",
			},
			observedConstraint{Table: table.name, Name: "pk_" + table.name, Type: "PRIMARY KEY", Columns: []string{"id"}},
			observedConstraint{Table: table.name, Name: "uq_" + table.name + "_code", Type: "UNIQUE", Columns: []string{"code"}},
		)
	}
	return constraints
}

func dereferenceString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func hasText(value *string) bool {
	return value != nil && strings.TrimSpace(*value) != ""
}
