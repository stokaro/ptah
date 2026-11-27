package devcollation_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/internal/devcollation"
)

// collation is a server that answers as a collation does: names that differ
// only in case, in accent, in width or in kana are one name where the
// matching sensitivity is off. A collation without _KS or _WS in its name
// reads full-width and half-width letters, and hiragana and katakana, as one
// name, so the zero value of those two fields is the common one.
type collation struct {
	dialect           string
	name              string
	caseInsensitive   bool
	accentInsensitive bool
	widthSensitive    bool
	kanaSensitive     bool
	err               error
	asked             *int
}

func (s collation) Info() catalog.ServerInfo { return catalog.ServerInfo{Dialect: s.dialect} }

func (s collation) ResolveIdentifierSemantics(_ context.Context, names []string) (identifier.Semantics, error) {
	if s.asked != nil {
		*s.asked++
	}
	if s.err != nil {
		return identifier.Semantics{}, s.err
	}
	var folds []string
	if s.accentInsensitive {
		folds = append(folds, "á", "a", "Á", "A")
	}
	if !s.widthSensitive {
		folds = append(folds, "Ｍ", "M")
	}
	if !s.kanaSensitive {
		folds = append(folds, "ア", "あ")
	}
	class := func(name string) string {
		name = strings.NewReplacer(folds...).Replace(name)
		if s.caseInsensitive {
			name = strings.ToLower(name)
		}
		return name
	}
	byClass := make(map[string]string)
	for _, name := range names {
		if current, found := byClass[class(name)]; !found || name < current {
			byClass[class(name)] = name
		}
	}
	resolved := make([]identifier.ResolvedName, 0, len(names))
	for _, name := range slices.Compact(slices.Sorted(slices.Values(names))) {
		resolved = append(resolved, identifier.ResolvedName{Name: name, Key: byClass[class(name)]})
	}
	return identifier.ForSQLServerCatalog(s.name).WithResolvedNames(resolved), nil
}

func TestRefuse_HappyPath(t *testing.T) {
	tests := []struct {
		name        string
		target, dev collation
	}{
		{
			name:   "two case-insensitive accent-sensitive collations of different names",
			target: collation{dialect: platform.SQLServer, name: "SQL_Latin1_General_CP1_CI_AS", caseInsensitive: true},
			dev:    collation{dialect: platform.SQLServer, name: "Latin1_General_100_CI_AS", caseInsensitive: true},
		},
		{
			name:   "two case-sensitive collations",
			target: collation{dialect: platform.SQLServer, name: "Latin1_General_100_CS_AS"},
			dev:    collation{dialect: platform.SQLServer, name: "Latin1_General_100_CS_AS"},
		},
		{
			name: "two width- and kana-sensitive collations",
			target: collation{
				dialect: platform.SQLServer, name: "Latin1_General_100_CI_AS_KS_WS", caseInsensitive: true,
				widthSensitive: true, kanaSensitive: true,
			},
			dev: collation{
				dialect: platform.SQLServer, name: "Japanese_XJIS_140_CI_AS_KS_WS", caseInsensitive: true,
				widthSensitive: true, kanaSensitive: true,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(devcollation.Refuse(t.Context(), test.target, test.dev, "the --dev-url database"), qt.IsNil)
		})
	}
}

// A dialect whose offline rules compare names asks neither server: the
// probe would cost a dialect such as MySQL a temporary table on the target.
func TestRefuse_AsksNothingOfADialectWithOfflineRules_HappyPath(t *testing.T) {
	c := qt.New(t)
	asked := 0
	target := collation{dialect: platform.Postgres, caseInsensitive: true, asked: &asked}
	dev := collation{dialect: platform.Postgres, asked: &asked}

	c.Assert(devcollation.Refuse(t.Context(), target, dev, "the --dev-url database"), qt.IsNil)
	c.Assert(asked, qt.Equals, 0)
}

func TestRefuse_FailurePath(t *testing.T) {
	tests := []struct {
		name        string
		target, dev collation
		want        string
	}{
		{
			name:   "a case-sensitive dev database for a case-insensitive target",
			target: collation{dialect: platform.SQLServer, name: "SQL_Latin1_General_CP1_CI_AS", caseInsensitive: true},
			dev:    collation{dialect: platform.SQLServer, name: "Latin1_General_100_CS_AS"},
			want: `the --dev-url database compares names under the collation Latin1_General_100_CS_AS and the target under ` +
				`SQL_Latin1_General_CP1_CI_AS, which differ in case sensitivity, so a name the two read differently ` +
				`would build another state on the --dev-url database than the target holds; ` +
				`create the --dev-url database with COLLATE SQL_Latin1_General_CP1_CI_AS`,
		},
		{
			name:   "an accent-insensitive dev database for an accent-sensitive target",
			target: collation{dialect: platform.SQLServer, name: "Latin1_General_100_CI_AS", caseInsensitive: true},
			dev:    collation{dialect: platform.SQLServer, name: "Latin1_General_100_CI_AI", caseInsensitive: true, accentInsensitive: true},
			want: `the --dev-url database compares names under the collation Latin1_General_100_CI_AI and the target under ` +
				`Latin1_General_100_CI_AS, which differ in accent sensitivity, .*`,
		},
		{
			name:   "a width-sensitive dev database for a width-insensitive target",
			target: collation{dialect: platform.SQLServer, name: "SQL_Latin1_General_CP1_CI_AS", caseInsensitive: true},
			dev:    collation{dialect: platform.SQLServer, name: "Latin1_General_100_CI_AS_WS", caseInsensitive: true, widthSensitive: true},
			want:   `.*, which differ in width sensitivity, .*; create the --dev-url database with COLLATE SQL_Latin1_General_CP1_CI_AS`,
		},
		{
			name:   "a kana-sensitive target for a kana-insensitive dev database",
			target: collation{dialect: platform.SQLServer, name: "Japanese_CI_AS_KS", caseInsensitive: true, kanaSensitive: true},
			dev:    collation{dialect: platform.SQLServer, name: "Japanese_CI_AS", caseInsensitive: true},
			want:   `.*, which differ in kana sensitivity, .*; create the --dev-url database with COLLATE Japanese_CI_AS_KS`,
		},
		{
			name:   "two sensitivities",
			target: collation{dialect: platform.SQLServer, name: "Latin1_General_100_CS_AS"},
			dev:    collation{dialect: platform.SQLServer, name: "Latin1_General_100_CI_AI", caseInsensitive: true, accentInsensitive: true},
			want:   `.*, which differ in case and accent sensitivity, .*`,
		},
		{
			name: "every sensitivity",
			target: collation{
				dialect: platform.SQLServer, name: "Latin1_General_100_CS_AS_KS_WS", widthSensitive: true, kanaSensitive: true,
			},
			dev:  collation{dialect: platform.SQLServer, name: "Latin1_General_100_CI_AI", caseInsensitive: true, accentInsensitive: true},
			want: `.*, which differ in case, accent, width and kana sensitivity, .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(devcollation.Refuse(t.Context(), test.target, test.dev, "the --dev-url database"), qt.ErrorMatches, test.want)
		})
	}
}

func TestRefuse_ServerError_FailurePath(t *testing.T) {
	failed := errors.New("login failed")
	tests := []struct {
		name        string
		target, dev collation
		want        string
	}{
		{
			name:   "the target",
			target: collation{dialect: platform.SQLServer, err: failed},
			dev:    collation{dialect: platform.SQLServer},
			want:   `compare the collations of the target and the shadow database: read the target: login failed`,
		},
		{
			name:   "the dev database",
			target: collation{dialect: platform.SQLServer},
			dev:    collation{dialect: platform.SQLServer, err: failed},
			want:   `compare the collations of the target and the shadow database: read the shadow database: login failed`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := devcollation.Refuse(t.Context(), test.target, test.dev, "the shadow database")
			c.Assert(err, qt.ErrorIs, failed)
			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}
