//go:build integration

package dbschema_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/dbtarget"
)

// The pairs the two engines answer oppositely. Every non-ASCII rune is written
// as an escape rather than pasted, because the Kelvin sign does not survive an
// ordinary editor or shell round trip: written literally it arrives as ASCII
// "K", and the row then compares a name with itself and passes against any
// implementation at all. The measurement this test re-runs was thrown away
// three times for exactly that class of reason.
const (
	liveDottedCapitalI = "\u0130" // LATIN CAPITAL LETTER I WITH DOT ABOVE
	liveDotlessI       = "\u0131" // LATIN SMALL LETTER DOTLESS I
	liveCapitalSigma   = "\u03A3" // GREEK CAPITAL LETTER SIGMA
	liveFinalSigma     = "\u03C2" // GREEK SMALL LETTER FINAL SIGMA
	liveKelvinSign     = "\u212A" // KELVIN SIGN
	liveADiaeresis     = "\u00E4" // LATIN SMALL LETTER A WITH DIAERESIS
)

// TestMySQLLiveIndexNameEquivalence_TheServerAnswersWhatNoFoldCan is
// stokaro/ptah#2768 asked of the servers rather than remembered from a table.
//
// The acceptance for that issue is these five rows against both engines, and
// nothing re-measured them: the offline tests encode the answers, so a server
// that changed its mind would leave every one of them green. This is the test
// that reddens.
//
// It drives [dbschema.DatabaseConnection.ResolveIdentifierSemantics], which is
// the entry point every database-aware comparison already calls, so what it
// measures is the answer a real `ptah schema compare` would use rather than a
// probe written beside it.
//
// The rows are not symmetric between the engines and that is the finding:
// MySQL treats `İ`/`i` and `K`(U+212A)/`K` as one name each, MariaDB treats
// `I`/`ı` and `Σ`/`ς` as one name each, and each accepts what the other
// refuses. No offline fold produces both answers, and none of the collations
// either engine exposes produces MariaDB's.
// TestLiveFixtureRunesAreTheCodePointsTheyName asserts this file's own inputs
// before any server is asked.
//
// The Kelvin sign written literally arrives as ASCII "K". When it did, the
// MySQL rows passed by comparing a name with itself and only MariaDB reddened,
// so the artifact looked like a MariaDB defect. A fixture that proves its own
// bytes is what separates the two.
func TestLiveFixtureRunesAreTheCodePointsTheyName(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  rune
	}{
		{name: "dotted capital I", value: liveDottedCapitalI, want: 0x0130},
		{name: "dotless i", value: liveDotlessI, want: 0x0131},
		{name: "capital sigma", value: liveCapitalSigma, want: 0x03A3},
		{name: "final sigma", value: liveFinalSigma, want: 0x03C2},
		{name: "kelvin sign", value: liveKelvinSign, want: 0x212A},
		{name: "a with diaeresis", value: liveADiaeresis, want: 0x00E4},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert([]rune(test.value), qt.DeepEquals, []rune{test.want})
		})
	}
}

func TestMySQLLiveIndexNameEquivalence_TheServerAnswersWhatNoFoldCan(t *testing.T) {
	tests := []struct {
		name   string
		engine dbtarget.Engine
		// collide and apart are the pairs this engine joins and separates.
		collide [][2]string
		apart   [][2]string
	}{
		{
			name:   "mysql",
			engine: dbtarget.MySQL,
			collide: [][2]string{
				{liveDottedCapitalI, "i"},
				{liveKelvinSign, "K"},
			},
			apart: [][2]string{
				{liveDotlessI, "I"},
				{liveFinalSigma, liveCapitalSigma},
				{liveADiaeresis, "a"},
			},
		},
		{
			name:   "mariadb",
			engine: dbtarget.MariaDB,
			collide: [][2]string{
				{liveDotlessI, "I"},
				{liveFinalSigma, liveCapitalSigma},
			},
			apart: [][2]string{
				{liveDottedCapitalI, "i"},
				{liveKelvinSign, "K"},
				{liveADiaeresis, "a"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx := t.Context()

			conn, err := dbschema.ConnectToDatabase(ctx, dbtarget.URL(t, test.engine))
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(conn)

			names := []string{
				"a", "i", "I", "K",
				liveADiaeresis, liveCapitalSigma, liveDotlessI,
				liveDottedCapitalI, liveFinalSigma, liveKelvinSign,
			}
			semantics, err := conn.ResolveIdentifierSemantics(ctx, names)
			c.Assert(err, qt.IsNil)

			for _, pair := range test.collide {
				c.Assert(semantics.IndexConflictKey(pair[0]), qt.Equals,
					semantics.IndexConflictKey(pair[1]),
					qt.Commentf("%s treats %q and %q as one index name", test.name, pair[0], pair[1]))
			}
			for _, pair := range test.apart {
				c.Assert(semantics.IndexConflictKey(pair[0]), qt.Not(qt.Equals),
					semantics.IndexConflictKey(pair[1]),
					qt.Commentf("%s keeps %q and %q apart", test.name, pair[0], pair[1]))
			}
		})
	}
}

// TestMySQLLiveIndexNameEquivalence_AnASCIIOnlySchemaAsksNothing is the control
// that keeps the cost claim honest.
//
// The probe exists for names Ptah cannot fold, and an ordinary schema has none.
// If an ASCII-only candidate set reached the server, every comparison against
// every MySQL database would pay for a temporary table, and the resolution
// would fail wherever the connected account may not create one.
//
// Asserting the answer rather than counting statements: an ASCII-only set comes
// back with the offline semantics, which is the value a connection that asked
// nothing would carry.
func TestMySQLLiveIndexNameEquivalence_AnASCIIOnlySchemaAsksNothing(t *testing.T) {
	tests := []struct {
		name   string
		engine dbtarget.Engine
	}{
		{name: "mysql", engine: dbtarget.MySQL},
		{name: "mariadb", engine: dbtarget.MariaDB},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx := t.Context()

			conn, err := dbschema.ConnectToDatabase(ctx, dbtarget.URL(t, test.engine))
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(conn)

			semantics, err := conn.ResolveIdentifierSemantics(ctx,
				[]string{"idx_users_email", "IDX_USERS_EMAIL", "orders"})
			c.Assert(err, qt.IsNil)

			// Folded, because both engines fold ASCII and so does Ptah, and
			// resolved-mode is not what answered.
			c.Assert(semantics.IndexConflictKey("idx_users_email"), qt.Equals,
				semantics.IndexConflictKey("IDX_USERS_EMAIL"))
			c.Assert(semantics.IndexConflictUnresolved("idx_users_email"), qt.IsFalse)
		})
	}
}

// TestMySQLLiveIndexNameEquivalence_ARefusalIsNotAnEquivalence_FailurePath is
// the half a mutation found missing.
//
// The probe learns that two names are one by being refused, so every refusal
// looks alike from a distance. Reading a refusal that is NOT the duplicate-name
// answer as a collision would make a lost connection, or an account that may
// not create a temporary table, arrive as "the server says all these names are
// the same" -- a guess wearing the server's authority, which is what this issue
// exists to stop.
//
// Measured on mysql:8.4.11 and mariadb:11.8.9: an account holding SELECT,
// CREATE, DROP, INDEX and ALTER but not CREATE TEMPORARY TABLES answers
// `ERROR 1044 (42000): Access denied` to the probe. That is the failure this
// asserts is reported rather than folded into an answer.
//
// It is a live test because the distinction is between two server refusals, and
// a fake that returns whichever error the test chose would be asserting the
// test's own fixture.
func TestMySQLLiveIndexNameEquivalence_ARefusalIsNotAnEquivalence_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		engine dbtarget.Engine
	}{
		{name: "mysql", engine: dbtarget.MySQLAdmin},
		{name: "mariadb", engine: dbtarget.MariaDBAdmin},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx := t.Context()

			adminURL := dbtarget.URL(t, test.engine)
			admin, err := dbschema.ConnectToDatabase(ctx, adminURL)
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(admin)

			account := fmt.Sprintf("ptah_notemp_%d", time.Now().UnixNano()%1000000)
			grantProbeless(c, ctx, admin, account)
			defer revokeProbeless(c, context.Background(), admin, account)

			restricted, err := dbschema.ConnectToDatabase(ctx,
				replaceCredentials(adminURL, account, "ptah"))
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(restricted)

			_, err = restricted.ResolveIdentifierSemantics(ctx,
				[]string{"a", liveADiaeresis})

			c.Assert(err, qt.IsNotNil,
				qt.Commentf("a refusal the probe cannot read must not become an equivalence"))
			c.Assert(err.Error(), qt.Contains, "resolve MySQL-family index names")
		})
	}
}

// grantProbeless creates an account that may read and write schema objects and
// may not create the temporary table the probe needs.
func grantProbeless(c *qt.C, ctx context.Context, admin *dbschema.DatabaseConnection, account string) {
	c.Helper()
	_, err := admin.ExecContext(ctx,
		fmt.Sprintf("CREATE USER '%s'@'%%' IDENTIFIED BY 'ptah'", account))
	c.Assert(err, qt.IsNil)
	_, err = admin.ExecContext(ctx,
		fmt.Sprintf("GRANT SELECT, CREATE, DROP, INDEX, ALTER ON *.* TO '%s'@'%%'", account))
	c.Assert(err, qt.IsNil)
}

// revokeProbeless removes the account, and only Checks so cleanup cannot mask a
// failure the test already reported.
func revokeProbeless(c *qt.C, ctx context.Context, admin *dbschema.DatabaseConnection, account string) {
	c.Helper()
	_, err := admin.ExecContext(ctx, fmt.Sprintf("DROP USER IF EXISTS '%s'@'%%'", account))
	c.Check(err, qt.IsNil)
}
