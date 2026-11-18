package migrator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/migrator"
)

// A refusal names the table, its owner and the connection's role, and says
// whether the table stands there or is one Ptah has not created yet, since
// the remedies differ: a table that stands can be dropped, and one that does
// not belongs in another schema.
func TestForeignMetadataTableError(t *testing.T) {
	tests := []struct {
		name   string
		absent bool
		want   string
	}{
		{
			name: "existing",
			want: `refusing to use metadata table "OWNER"."PTAH_LOCKS": it is owned by "OWNER" and this connection runs as "APP", ` +
				`so it is not the table Ptah would have created. .*set PTAH_ALLOW_FOREIGN_METADATA_TABLE=1`,
		},
		{
			name:   "absent",
			absent: true,
			want: `refusing to create metadata table "OWNER"."PTAH_LOCKS": its schema belongs to "OWNER" and this connection runs as "APP", ` +
				`so the table would be "OWNER"'s. .*set PTAH_ALLOW_FOREIGN_METADATA_TABLE=1`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := error(&migrator.ForeignMetadataTableError{Table: `"OWNER"."PTAH_LOCKS"`, Owner: "OWNER", Role: "APP", Absent: test.absent})
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(err, qt.ErrorIs, migrator.ErrForeignMetadataTable)
		})
	}
}
