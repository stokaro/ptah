package schemaselection_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/schemaselection"
)

// TestURLScopeAnswersFromTheURLAlone pins which URLs limit a run to one schema.
//
// Each row that matters is a measurement against the pinned Atlas community
// binary v1.3.0 with `schema inspect -u file://<two schema blocks>.hcl`, exit
// codes read from unpiped invocations and a throwaway database per run:
//
//	--dev-url sqlite://dv?mode=memory                   exit 1, limited to "main"
//	--dev-url postgres://…/db?search_path=public        exit 1, limited to "public"
//	--dev-url mysql://…/wf823_dev                       exit 1, limited to "wf823_dev"
//	--dev-url postgres://…/db (no search_path)          exit 0
//
// The PostgreSQL realm row is the one that decides whether the gate above is
// usable at all: an implementation that read the DATABASE name there -- which
// is what the MySQL row does -- would call every PostgreSQL URL limited and
// refuse the realm-scoped multi-schema document that binary loads at exit 0.
//
// The comma row is not a search-path list to that binary but one schema NAME,
// which it refuses outright (`schema "public,app" was not found`), so nothing
// is limited here and the whole document stays under review.
func TestURLScopeAnswersFromTheURLAlone(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		scope   string
		limited bool
	}{
		{name: "sqlite is always one namespace", url: "sqlite://dv?mode=memory", scope: "main", limited: true},
		{name: "sqlite file", url: "sqlite://app.db", scope: "main", limited: true},
		{
			name:    "postgres with a single search_path",
			url:     "postgres://localhost:5432/db?sslmode=disable&search_path=public",
			scope:   "public",
			limited: true,
		},
		{name: "postgres without a search_path", url: "postgres://localhost:5432/db?sslmode=disable", scope: "", limited: false},
		{
			name:    "postgres with a comma-carrying search_path limits nothing",
			url:     "postgres://localhost:5432/db?sslmode=disable&search_path=public,app",
			scope:   "",
			limited: false,
		},
		{name: "mysql names its database", url: "mysql://localhost:3306/appdb", scope: "appdb", limited: true},
		{name: "mysql with query parameters", url: "mysql://localhost:3306/appdb?parseTime=true", scope: "appdb", limited: true},
		{name: "mysql with no database", url: "mysql://localhost:3306/", scope: "", limited: false},
		{name: "mariadb names its database", url: "mariadb://localhost:3306/appdb", scope: "appdb", limited: true},
		{
			name:    "the mysql driver's tcp() host spelling still names its database",
			url:     "mysql://root@tcp(localhost:3306)/appdb",
			scope:   "appdb",
			limited: true,
		},
		{name: "an empty url limits nothing", url: "", scope: "", limited: false},
		{name: "an unparseable url limits nothing", url: "not a url", scope: "", limited: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			scope, limited := schemaselection.URLScope(test.url)

			c.Assert(scope, qt.Equals, test.scope)
			c.Assert(limited, qt.Equals, test.limited)
		})
	}
}
