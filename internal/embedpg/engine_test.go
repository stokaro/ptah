package embedpg_test

import (
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/embedpg"
	"ptah.run/internal/ydbgap"
)

// A URL the PostgreSQL driver reads is left to it: a PostgreSQL scheme, a
// keyword/value DSN with no scheme, and a scheme Ptah does not recognize.
func TestRefuseAnotherEngine_HappyPath(t *testing.T) {
	for _, dbURL := range []string{
		"postgres://localhost:5432/db",
		"postgresql://localhost:5432/db",
		"host=localhost user=ptah dbname=ptah",
		"unknown://localhost/db",
	} {
		t.Run(dbURL, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(embedpg.RefuseAnotherEngine(dbURL), qt.IsNil)
		})
	}
}

// A YDB URL names the gap that plans inference on YDB, under either scheme;
// another engine's URL says there is nothing to run against it.
func TestRefuseAnotherEngine_FailurePath(t *testing.T) {
	tests := []struct {
		name  string
		dbURL string
		want  string
	}{
		{name: "ydb", dbURL: "ydb://localhost:2136/local",
			want: `"ydb://" names a YDB database: ` + regexp.QuoteMeta(ydbgap.Inference.Message())},
		{name: "ydbs", dbURL: "ydbs://ydb.example.com:2135/?database=/a/b",
			want: `"ydbs://" names a YDB database: ` + regexp.QuoteMeta(ydbgap.Inference.Message())},
		{name: "mysql", dbURL: "mysql://localhost:3306/db",
			want: `ptah inference works against PostgreSQL with pgvector, and "mysql://" names another engine: ` +
				`a generation's run state and its vectors are a PostgreSQL vertical, so there is nothing here to ` +
				`run against mysql`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(embedpg.RefuseAnotherEngine(test.dbURL), qt.ErrorMatches, test.want)
		})
	}
}
