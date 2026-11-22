//go:build integration

package probetext_test

import (
	"context"
	"database/sql"
	"errors"
	"math/rand/v2"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/probetext"
)

// differentialEngines are the PostgreSQL-family engines, each with the
// dialect the probe text check reads its text for.
var differentialEngines = []struct {
	name    string
	engine  dbtarget.Engine
	dialect string
}{
	{name: "PostgreSQL", engine: dbtarget.PostgreSQL, dialect: platform.Postgres},
	{name: "CockroachDB", engine: dbtarget.CockroachDB, dialect: platform.CockroachDB},
	{name: "YugabyteDB", engine: dbtarget.YugabyteDB, dialect: platform.YugabyteDB},
}

// differentialPieces are the pieces the differential corpus joins: strings,
// comments, dollar quotes and names in the spellings on which a lexer and a
// PostgreSQL-family server have disagreed about where a string or a name
// ends. Each is followed by a space when it is joined, and some end in a line
// break of their own; the last start with a space outside ASCII, which the
// servers read as part of a name even after an ASCII space.
var differentialPieces = []string{
	"1", "1 AS x", `'\'`, `'\''`, `E'\''`, `e'\''`, `b'\''`, `B'\''`, `x'0'`, `U&'\0041'`,
	"$a$ $a$", "$$ $$", "x€$a$", "x $a$", "x́$a$", "ε$a$", "٣$a$", "x　$a$",
	"AS y€$a$", "AS y $a$", "--'\n", "/* ' */", "/* /* */ */", "E'a'\n'\\''", "'a'\n'\\''",
	"E'a' --c\n'\\''", `'\'' --'` + "\n", `AS "x;"`, "-- '\n", "\r--'\n", "-- \r'\n", `1e'\''`, `$1e'\''`,
	"$1$a$", "1 AS $a$", "AS x$a$", "U&'a' UESCAPE '!'", `E'\\'`, "'x'", "''", "AS y", "$a$", "'", "E'",
	"\u00a0$a$", "\u0085$a$", "\u2028$a$", "\u3000$a$",
}

// differentialCorpus is texts of two statements as a lexer might not see
// them: `SELECT a b ; SELECT c d` over pieces drawn with a fixed seed, so a
// run reads the same corpus every time.
func differentialCorpus(size int) []string {
	draw := rand.New(rand.NewPCG(6462, 7)) // #nosec G404 -- A fixed corpus, not a secret.
	pick := func() string { return differentialPieces[draw.IntN(len(differentialPieces))] }
	seen := make(map[string]bool, size)
	corpus := make([]string, 0, size)
	for range size {
		text := "SELECT " + pick() + " " + pick() + " ; SELECT " + pick() + " " + pick()
		if !seen[text] {
			seen[text] = true
			corpus = append(corpus, text)
		}
	}
	return corpus
}

// acceptedTexts is the texts of corpus the probe text check accepts as one
// statement for dialect.
func acceptedTexts(dialect string, corpus []string) []string {
	var accepted []string
	for _, text := range corpus {
		if probetext.Statement(dialect, text) == nil {
			accepted = append(accepted, text)
		}
	}
	return accepted
}

// splitTexts is the texts the server reads as more than one statement when
// it prepares them on conn.
func splitTexts(ctx context.Context, conn *sql.Conn, texts []string) ([]string, error) {
	var split []string
	for _, text := range texts {
		prepared, err := conn.PrepareContext(ctx, text)
		if err == nil {
			if closeErr := prepared.Close(); closeErr != nil {
				return nil, closeErr
			}
			continue
		}
		if severalStatements(err) {
			split = append(split, text)
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	return split, nil
}

// severalStatements reports whether err is a server's refusal to prepare
// text that holds more than one statement: SQLSTATE 42P14 on CockroachDB,
// 42601 with this message on PostgreSQL and YugabyteDB.
func severalStatements(err error) bool {
	var coded interface{ SQLState() string }
	if !errors.As(err, &coded) {
		return false
	}
	switch coded.SQLState() {
	case "42P14":
		return true
	case "42601":
		return strings.Contains(err.Error(), "multiple commands")
	default:
		return false
	}
}

// differentialFloor is the fewest texts the check must accept for the
// differential to say anything: a corpus the check refused whole would find
// no disagreement and pass.
const differentialFloor = 1000

// TestStatement_LiveCheckAndServerAgreeOnOneStatement prepares every text of
// a fixed corpus that the probe text check accepts as one statement, on each
// PostgreSQL-family server, and finds none the server reads as two. Each text
// holds a semicolon the check reads inside a string, a quoted name or a
// comment; where the server reads it outside, a declaration spelled that way
// would add a statement of its own to the probe. Every probe statement also
// runs prepared, so the server refuses such a text; this test holds the check
// to the server as well, which is what keeps the declaration's warning
// accurate.
func TestStatement_LiveCheckAndServerAgreeOnOneStatement(t *testing.T) {
	corpus := differentialCorpus(12000)
	for _, test := range differentialEngines {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := sql.Open("pgx", dbtarget.DriverDSN(c, test.engine))
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
			conn, err := db.Conn(c.Context())
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
			accepted := acceptedTexts(test.dialect, corpus)

			split, err := splitTexts(c.Context(), conn, accepted)

			c.Assert(err, qt.IsNil)
			c.Assert(len(accepted) >= differentialFloor, qt.IsTrue, qt.Commentf("accepted %d of %d", len(accepted), len(corpus)))
			c.Assert(split, qt.DeepEquals, []string(nil))
		})
	}
}
