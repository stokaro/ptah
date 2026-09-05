package indexbacking_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/internal/indexbacking"
)

func TestKindOf_HappyPath(t *testing.T) {
	tests := []struct {
		name           string
		constraintType string
		want           indexbacking.Kind
	}{
		{name: "primary key", constraintType: "PRIMARY KEY", want: indexbacking.PrimaryKey},
		{name: "unique", constraintType: "UNIQUE", want: indexbacking.Unique},
		{name: "exclude", constraintType: "EXCLUDE", want: indexbacking.Exclusion},
		{name: "foreign key", constraintType: "FOREIGN KEY", want: indexbacking.ForeignKey},
		{name: "lower case", constraintType: "unique", want: indexbacking.Unique},
		{name: "surrounded by space", constraintType: "  PRIMARY KEY  ", want: indexbacking.PrimaryKey},
		{name: "check", constraintType: "CHECK", want: indexbacking.None},
		{name: "empty", constraintType: "", want: indexbacking.None},
		{name: "unrecognized", constraintType: "SOMETHING ELSE", want: indexbacking.None},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(indexbacking.KindOf(test.constraintType), qt.Equals, test.want)
		})
	}
}

func TestServerBacks_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		kind    indexbacking.Kind
		want    bool
	}{
		{name: "postgres unique", dialect: "postgres", kind: indexbacking.Unique, want: true},
		{name: "mysql unique", dialect: "mysql", kind: indexbacking.Unique, want: true},
		{name: "sqlserver keeps them separate", dialect: "sqlserver", kind: indexbacking.Unique, want: false},
		{name: "postgres exclusion", dialect: "postgres", kind: indexbacking.Exclusion, want: true},
		{name: "mysql foreign key", dialect: "mysql", kind: indexbacking.ForeignKey, want: true},
		{name: "mariadb foreign key", dialect: "mariadb", kind: indexbacking.ForeignKey, want: true},
		{name: "spanner foreign key", dialect: "spanner", kind: indexbacking.ForeignKey, want: true},
		{name: "postgres creates no foreign key index", dialect: "postgres", kind: indexbacking.ForeignKey, want: false},
		{name: "a check has no index", dialect: "postgres", kind: indexbacking.None, want: false},
		{name: "an unknown dialect answers what every server does", dialect: "", kind: indexbacking.Unique, want: true},
		{name: "an unknown dialect claims no foreign key index", dialect: "", kind: indexbacking.ForeignKey, want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(indexbacking.ServerBacks(test.dialect, test.kind), qt.Equals, test.want)
		})
	}
}

// TestNamedAfterConstraint_IsNarrowerThanServerBacks pins the gap between the
// two, which is the whole reason both exist.
//
// A foreign key's backing index carries the constraint's name on the three
// servers that create one, so ServerBacks answers true -- and an ordinary index
// may carry that name too, so a caller matching on the name alone would suppress
// a user's own index. The comparator matches on the constraint's columns as
// well; a caller that cannot must not attribute one from the name.
func TestNamedAfterConstraint_IsNarrowerThanServerBacks(t *testing.T) {
	tests := []struct {
		name        string
		dialect     string
		kind        indexbacking.Kind
		serverBacks bool
		namedAfter  bool
	}{
		{name: "mysql foreign key", dialect: "mysql", kind: indexbacking.ForeignKey, serverBacks: true, namedAfter: false},
		{name: "mariadb foreign key", dialect: "mariadb", kind: indexbacking.ForeignKey, serverBacks: true, namedAfter: false},
		{name: "spanner foreign key", dialect: "spanner", kind: indexbacking.ForeignKey, serverBacks: true, namedAfter: false},
		{name: "primary key is read from its mark", dialect: "postgres", kind: indexbacking.PrimaryKey, serverBacks: true, namedAfter: false},
		{name: "postgres unique agrees", dialect: "postgres", kind: indexbacking.Unique, serverBacks: true, namedAfter: true},
		{name: "postgres exclusion agrees", dialect: "postgres", kind: indexbacking.Exclusion, serverBacks: true, namedAfter: true},
		{name: "sqlserver unique agrees on false", dialect: "sqlserver", kind: indexbacking.Unique, serverBacks: false, namedAfter: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(indexbacking.ServerBacks(test.dialect, test.kind), qt.Equals, test.serverBacks)
			c.Assert(indexbacking.NamedAfterConstraint(test.dialect, test.kind), qt.Equals, test.namedAfter)
		})
	}
}

func TestUnaddressable_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		index   catalog.Index
		dialect string
		want    bool
	}{
		{
			name:    "a primary key index belongs to the key",
			index:   catalog.Index{Name: "users_pkey", IsPrimary: true},
			dialect: "postgres",
			want:    true,
		},
		{
			name:    "sqlite names its own backing structure",
			index:   catalog.Index{Name: "sqlite_autoindex_users_1", IsUnique: true},
			dialect: "sqlite",
			want:    true,
		},
		{
			name:    "an ordinary index is addressable",
			index:   catalog.Index{Name: "idx_users_email", IsUnique: true},
			dialect: "sqlite",
			want:    false,
		},
		{
			name:    "the sqlite prefix is not read on another server",
			index:   catalog.Index{Name: "sqlite_autoindex_users_1", IsUnique: true},
			dialect: "postgres",
			want:    false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(indexbacking.Unaddressable(test.index, test.dialect), qt.Equals, test.want)
		})
	}
}
