package embedpg

import (
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/ydbgap"
)

// RefuseAnotherEngine says what an inference migration speaks to, before the
// PostgreSQL driver says it cannot parse a URL.
//
// Every caller opens its database on the PostgreSQL driver directly, because
// the run-state tables and the vector catalogs this package reads have no
// dialect-agnostic form. So another engine's URL is refused either way -- by
// pgx failing to parse it, with `cannot parse ...: invalid keyword/value`,
// which reads as a malformed connection string and sends an operator to check
// one that is correct (stokaro/ptah#2386). `ptah inference` and the agent
// surface's inference tools both ask this before they dial, so the two give
// one answer.
//
// It refuses only a scheme Ptah RECOGNIZES as another dialect. An unrecognized
// scheme, and a keyword/value DSN with no scheme at all, still reach the driver:
// `host=localhost user=ptah` is a form pgx accepts and this must not start
// rejecting it for having no scheme to read.
//
// YDB is refused through its gap rather than as an engine with nothing to run
// against: an inference migration on YDB's vector indexes is planned.
func RefuseAnotherEngine(dbURL string) error {
	scheme, _, found := strings.Cut(dbURL, "://")
	if !found {
		return nil
	}
	dialect := platform.NormalizeDialect(strings.ToLower(scheme))
	switch dialect {
	case "", platform.Postgres:
		return nil
	case platform.YDB:
		return fmt.Errorf("%q names a YDB database: %s", scheme+"://", ydbgap.Inference.Message())
	}
	// The dialect is named once. Saying it mid-sentence as well would put the
	// same answer in two places, and neither could then be measured: remove
	// either and the other still carries it.
	return fmt.Errorf(
		"ptah inference works against PostgreSQL with pgvector, and %q names another engine: "+
			"a generation's run state and its vectors are a PostgreSQL vertical, "+
			"so there is nothing here to run against %s",
		scheme+"://", dialect)
}
