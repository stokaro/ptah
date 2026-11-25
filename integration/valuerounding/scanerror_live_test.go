//go:build integration

package valuerounding_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/url"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/planrefusal"
)

// reader creates a role on the PostgreSQL server that may read columns of
// prices in database, and returns the address it connects with. The role is
// dropped when the test ends.
func reader(c *qt.C, database, columns string) string {
	c.Helper()
	random := make([]byte, 6)
	_, err := rand.Read(random)
	c.Assert(err, qt.IsNil)
	role := "ptah_rounding_reader_" + hex.EncodeToString(random)
	secret := make([]byte, 12)
	_, err = rand.Read(secret)
	c.Assert(err, qt.IsNil)
	password := hex.EncodeToString(secret)
	admin := dbtarget.URL(c, dbtarget.PostgreSQL)
	run(c, admin, `CREATE ROLE "`+role+`" LOGIN PASSWORD '`+password+`'`)
	c.Cleanup(func() {
		conn, err := dbschema.ConnectToDatabase(context.Background(), admin)
		c.Check(err, qt.IsNil)
		defer dbschema.CloseAndWarn(conn)
		_, err = conn.ExecContext(context.Background(), `DROP ROLE "`+role+`"`)
		c.Check(err, qt.IsNil)
	})
	run(c, database, `GRANT SELECT (`+columns+`) ON prices TO "`+role+`"`)
	c.Cleanup(func() {
		conn, err := dbschema.ConnectToDatabase(context.Background(), database)
		c.Check(err, qt.IsNil)
		defer dbschema.CloseAndWarn(conn)
		_, err = conn.ExecContext(context.Background(), `DROP OWNED BY "`+role+`"`)
		c.Check(err, qt.IsNil)
	})
	address, err := url.Parse(database)
	c.Assert(err, qt.IsNil)
	address.User = url.UserPassword(role, password)
	return address.String()
}

// twoScales declares prices.amount and prices.secret with one digit after the
// point.
func twoScales() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Price", Name: "prices"}},
		Fields: []schemamodel.Field{
			{StructName: "Price", Name: "id", Type: "int", Primary: true},
			{StructName: "Price", Name: "amount", Type: "numeric(8,1)", Nullable: true},
			{StructName: "Price", Name: "secret", Type: "numeric(8,1)", Nullable: true},
		},
	}
}

// TestPlanReportsAReadThatFailedForAnotherReasonLive plans two lower scales as a
// role that may read one of the two columns. The scan of the table fails, the
// count of its rows does not, and the column the role may not read fails alone
// with the server's refusal of the privilege, not of a conversion. The plan
// ends with that error as it is, rather than a refusal that blames the new type.
func TestPlanReportsAReadThatFailedForAnotherReasonLive(t *testing.T) {
	c := qt.New(t)
	target := postgresServer.scratch(c, "CREATE TABLE prices (id int PRIMARY KEY, amount numeric(8,3), secret numeric(8,3))",
		"INSERT INTO prices VALUES (4, 1.5, 2.5)")
	conn, err := dbschema.ConnectToDatabase(c.Context(), reader(c, target, "id, amount"))
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	_, err = atlasschema.PlanApply(c.Context(), conn, atlasschema.ApplyOptions{Desired: twoScales(), Runtime: must.Must(builtin.New())})

	c.Assert(err, qt.ErrorMatches, `(?s)read the values of column "secret" of table "public.prices": .*permission denied for table prices.*`)
	c.Assert(err, qt.Not(qt.ErrorAs), new(*planrefusal.RoundedValuesError))
}

// TestPlanNamesAValueTheNewTypeCannotHoldAsARoleThatReadsItLive is the control:
// the same role may read both columns, and a value the new scale cannot hold
// is named as the server's refusal to convert it.
func TestPlanNamesAValueTheNewTypeCannotHoldAsARoleThatReadsItLive(t *testing.T) {
	c := qt.New(t)
	target := postgresServer.scratch(c, "CREATE TABLE prices (id int PRIMARY KEY, amount numeric(8,3), secret numeric(5,3))",
		"INSERT INTO prices VALUES (4, 1.5, 99.999)")
	conn, err := dbschema.ConnectToDatabase(c.Context(), reader(c, target, "id, amount, secret"))
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	desired := twoScales()
	desired.Fields[2].Type = "numeric(3,1)"

	_, err = atlasschema.PlanApply(c.Context(), conn, atlasschema.ApplyOptions{Desired: desired, Runtime: must.Must(builtin.New())})

	c.Assert(err, qt.ErrorAs, new(*planrefusal.RoundedValuesError))
	c.Assert(err, qt.ErrorMatches, `(?s).*column "secret" of table "public.prices" from numeric\(5,3\) to numeric\(3,1\), where the server cannot convert a stored value to the new type: .*`)
}
