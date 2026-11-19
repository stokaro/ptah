package mysqlrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mysql/mysqlrender"
	"ptah.run/dialect/mysql/mysqlschema"
)

// TestCreateTableOptions_HappyPath writes the declared options over the ones a
// node carries, and keeps the node's own for a table that declares none.
func TestCreateTableOptions_HappyPath(t *testing.T) {
	declared := must.Must(schemaext.NewFacets(&mysqlschema.DesiredTable{Engine: "InnoDB", AutoIncrement: "100"}))
	tests := []struct {
		name    string
		facets  schemaext.Facets
		options map[string]string
		want    map[string]string
	}{
		{name: "declared options", facets: declared, options: map[string]string{"ENGINE": "MyISAM", "COLLATE": "utf8mb4_bin"},
			want: map[string]string{"ENGINE": "InnoDB", "AUTO_INCREMENT": "100", "COLLATE": "utf8mb4_bin"}},
		{name: "no declaration", options: map[string]string{"ENGINE": "MyISAM"}, want: map[string]string{"ENGINE": "MyISAM"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := mysqlrender.CreateTableOptions(test.facets, test.options)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestCreateTableOptions_FailurePath refuses a facet the MySQL family does not
// render, and an observation where a declaration belongs.
func TestCreateTableOptions_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		facets  schemaext.Facets
		wantErr string
	}{
		{name: "an observation", facets: must.Must(schemaext.NewFacets(&mysqlschema.ObservedTable{Charset: "utf8mb4"})),
			wantErr: `.*ptah.run/mysql/table.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := mysqlrender.CreateTableOptions(test.facets, nil)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.HasLen, 0)
		})
	}
}

// A PARTITION BY read as an option, from a document that names no target, is
// refused rather than written as PARTITION_BY=, which no server parses.
func TestCreateTableOptions_RefusesAPartitionOption(t *testing.T) {
	c := qt.New(t)

	got, err := mysqlrender.CreateTableOptions(schemaext.Facets{}, map[string]string{"ENGINE": "InnoDB", "PARTITION_BY": "HASH (id) PARTITIONS 2"})

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `unsupported feature: PARTITION BY: Ptah does not model MySQL or MariaDB partitioning \(stokaro/ptah#6357\); .*`)
	c.Assert(got, qt.IsNil)
}
