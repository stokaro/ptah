//go:build integration

package gonative_test

import (
	"database/sql"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/dbschema/postgres"
)

// TestPostgreSQLSequenceIntrospectionClassificationIntegration verifies the
// critical invariant of standalone-sequence introspection against a live
// database: SERIAL and identity backing sequences are excluded, while genuine
// standalone sequences — including one merely consumed via DEFAULT nextval and
// one carrying a lifecycle-only OWNED BY — are surfaced.
func TestPostgreSQLSequenceIntrospectionClassificationIntegration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)

	db, err := sql.Open("pgx", dsn)
	c.Assert(err, qt.IsNil)
	defer db.Close()

	cleanup := func() {
		for _, stmt := range []string{
			`DROP TABLE IF EXISTS seq_serial, seq_ident, seq_shared, seq_life CASCADE`,
			`DROP SEQUENCE IF EXISTS seq_shared_seq, seq_life_seq, seq_plain_seq CASCADE`,
		} {
			_, _ = db.Exec(stmt)
		}
	}
	cleanup()
	defer cleanup()

	for _, stmt := range []string{
		`CREATE TABLE seq_serial (id SERIAL PRIMARY KEY)`,                                   // implicit
		`CREATE TABLE seq_ident (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY)`,       // implicit
		`CREATE SEQUENCE seq_shared_seq`,                                                    // standalone
		`CREATE TABLE seq_shared (id bigint DEFAULT nextval('seq_shared_seq') PRIMARY KEY)`, // consumer, not owner
		`CREATE TABLE seq_life (id bigint PRIMARY KEY, other bigint)`,                       //
		`CREATE SEQUENCE seq_life_seq AS bigint START WITH 500 INCREMENT BY 2 CACHE 10`,     // standalone
		`ALTER SEQUENCE seq_life_seq OWNED BY seq_life.other`,                               // lifecycle-only owner
		`CREATE SEQUENCE seq_plain_seq`,                                                     // standalone
	} {
		_, err = db.Exec(stmt)
		c.Assert(err, qt.IsNil, qt.Commentf("statement failed: %s", stmt))
	}

	reader := postgres.NewPostgreSQLReader(db, "public")
	live, err := reader.ReadSchema()
	c.Assert(err, qt.IsNil)

	found := make(map[string]string, len(live.Sequences))
	for _, seq := range live.Sequences {
		found[seq.Name] = seq.OwnedBy
	}

	// Standalone sequences must be surfaced.
	_, hasShared := found["seq_shared_seq"]
	_, hasLife := found["seq_life_seq"]
	_, hasPlain := found["seq_plain_seq"]
	c.Assert(hasShared, qt.IsTrue, qt.Commentf("standalone sequence consumed via DEFAULT must be surfaced"))
	c.Assert(hasLife, qt.IsTrue, qt.Commentf("lifecycle-only OWNED BY sequence must be surfaced"))
	c.Assert(hasPlain, qt.IsTrue, qt.Commentf("plain standalone sequence must be surfaced"))

	// The lifecycle-only owner must round-trip as table.column.
	c.Assert(found["seq_life_seq"], qt.Equals, "seq_life.other")

	// Implicit SERIAL/identity backing sequences must be excluded.
	_, hasSerial := found["seq_serial_id_seq"]
	_, hasIdent := found["seq_ident_id_seq"]
	c.Assert(hasSerial, qt.IsFalse, qt.Commentf("SERIAL backing sequence must be excluded"))
	c.Assert(hasIdent, qt.IsFalse, qt.Commentf("identity backing sequence must be excluded"))
}
