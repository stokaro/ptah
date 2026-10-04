package datadiff_test

import (
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/datadiff"
)

// On YDB every value is written in its column's type, the table is one quoted
// path, and a NULL key is matched with IS NULL. The down script restores what
// the live rows held, in the types they came back as from the database.
func TestRenderStatements_YDB_HappyPath(t *testing.T) {
	c := qt.New(t)
	diff := &datadiff.DataDiff{
		Schema: "ref",
		Table:  "countries",
		Keys:   []string{"code"},
		ColumnTypes: map[string]string{
			"code": "Utf8", "population": "Int64", "rank": "Int32", "joined": "Timestamp", "flag": "String",
		},
		Inserts: []datadiff.Row{
			{"code": "CZ", "population": 10900000, "rank": 1, "joined": "2004-05-01", "flag": []byte{0xff, 0x00}},
		},
		Updates: []datadiff.RowUpdate{{
			Key:     map[string]any{"code": "US"},
			Desired: datadiff.Row{"code": "US", "population": 340000000, "rank": 2},
			Live:    datadiff.Row{"code": "US", "population": int64(330000000), "rank": int32(3)},
		}},
		Deletes: []datadiff.Row{{
			"code": nil, "population": int64(1), "rank": int32(9),
			"joined": time.Date(2020, time.January, 1, 1, 0, 0, 0, time.FixedZone("CET", 3600)),
		}},
	}

	up, down, err := datadiff.RenderStatements(diff, "ydb")

	c.Assert(err, qt.IsNil)
	c.Assert(up, qt.DeepEquals, []string{
		"INSERT INTO `ref/countries` (`code`, `flag`, `joined`, `population`, `rank`) " +
			`VALUES ('CZ'u, '\xff\x00', Timestamp('2004-05-01T00:00:00Z'), 10900000l, 1);`,
		"UPDATE `ref/countries` SET `population` = 340000000l, `rank` = 2 WHERE `code` = 'US'u;",
		"DELETE FROM `ref/countries` WHERE `code` IS NULL;",
	})
	c.Assert(down, qt.DeepEquals, []string{
		"INSERT INTO `ref/countries` (`code`, `joined`, `population`, `rank`) " +
			"VALUES (NULL, Timestamp('2020-01-01T00:00:00Z'), 1l, 9);",
		"UPDATE `ref/countries` SET `population` = 330000000l, `rank` = 3 WHERE `code` = 'US'u;",
		"DELETE FROM `ref/countries` WHERE `code` = 'CZ'u;",
	})
}
