package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
)

// TestDeclaredKeyName_HappyPath pins what a primary key a declaration names plans
// on a table the database holds. The key took `<table>_pkey` from the server, and
// the next comparison reported the schema synced.
func TestDeclaredKeyName_HappyPath(t *testing.T) {
	tests := []struct {
		name, held, desired string
		liveKey             []string
		want                []string
	}{
		{
			name:    "a table with no key gains a key the table names",
			held:    "CREATE TABLE parent (id bigint NOT NULL);",
			desired: "CREATE TABLE parent (id bigint NOT NULL, CONSTRAINT parent_pk PRIMARY KEY (id));",
			want:    []string{`ALTER TABLE "parent" ADD CONSTRAINT "parent_pk" PRIMARY KEY ("id")`},
		},
		{
			name:    "a table with no key gains a key the column names",
			held:    "CREATE TABLE parent (id bigint NOT NULL);",
			desired: "CREATE TABLE parent (id bigint CONSTRAINT parent_pk PRIMARY KEY);",
			want:    []string{`ALTER TABLE "parent" ADD CONSTRAINT "parent_pk" PRIMARY KEY ("id")`},
		},
		{
			name:    "a key moved to another column takes the name",
			held:    "CREATE TABLE parent (id bigint NOT NULL, code bigint NOT NULL);",
			desired: "CREATE TABLE parent (id bigint NOT NULL, code bigint NOT NULL, CONSTRAINT parent_pk PRIMARY KEY (code));",
			liveKey: []string{"id"},
			want: []string{
				`ALTER TABLE "parent" DROP CONSTRAINT "parent_pkey"`,
				`ALTER TABLE "parent" ADD CONSTRAINT "parent_pk" PRIMARY KEY ("code")`,
			},
		},
		{
			name:    "a moved key the declaration does not name keeps the live name",
			held:    "CREATE TABLE parent (id bigint NOT NULL, code bigint NOT NULL);",
			desired: "CREATE TABLE parent (id bigint NOT NULL, code bigint NOT NULL, PRIMARY KEY (code));",
			liveKey: []string{"id"},
			want: []string{
				`ALTER TABLE "parent" DROP CONSTRAINT "parent_pkey"`,
				`ALTER TABLE "parent" ADD PRIMARY KEY ("code")`,
			},
		},
		{
			name:    "a key over the same columns is not renamed",
			held:    "CREATE TABLE parent (id bigint NOT NULL);",
			desired: "CREATE TABLE parent (id bigint NOT NULL, CONSTRAINT parent_pk PRIMARY KEY (id));",
			liveKey: []string{"id"},
			want:    make([]string, 0),
		},
		{
			name:    "a changed UNIQUE on a table that names its key keeps its own name",
			held:    "CREATE TABLE parent (id bigint NOT NULL, code bigint, CONSTRAINT parent_u UNIQUE (id));",
			desired: "CREATE TABLE parent (id bigint NOT NULL, code bigint, CONSTRAINT parent_pk PRIMARY KEY (id), CONSTRAINT parent_u UNIQUE (code));",
			liveKey: []string{"id"},
			want: []string{
				`ALTER TABLE "parent" DROP CONSTRAINT "parent_u"`,
				`ALTER TABLE "parent" ADD CONSTRAINT "parent_u" UNIQUE ("code")`,
			},
		},
		{
			name:    "a name of the spelling the server would choose writes none",
			held:    "CREATE TABLE parent (id bigint NOT NULL);",
			desired: "CREATE TABLE parent (id bigint NOT NULL, CONSTRAINT parent_pkey PRIMARY KEY (id));",
			want:    []string{`ALTER TABLE "parent" ADD PRIMARY KEY ("id")`},
		},
		{
			name:    "a UNIQUE that holds the name leaves ahead of the key",
			held:    "CREATE TABLE parent (id bigint NOT NULL, CONSTRAINT parent_pk UNIQUE (id));",
			desired: "CREATE TABLE parent (id bigint NOT NULL, CONSTRAINT parent_pk PRIMARY KEY (id));",
			want: []string{
				`ALTER TABLE "parent" DROP CONSTRAINT "parent_pk"`,
				`ALTER TABLE "parent" ADD CONSTRAINT "parent_pk" PRIMARY KEY ("id")`,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(columnKeyPlan(c, platform.Postgres, test.held, test.desired, test.liveKey...), qt.DeepEquals, test.want)
		})
	}
}

// TestDeclaredKeyName_AServerThatNamesEveryKeyTakesNone is the control: MySQL
// calls every primary key PRIMARY whatever the statement says, so the plan writes
// no name, and a comparison under the declared name would plan the key on every
// run.
func TestDeclaredKeyName_AServerThatNamesEveryKeyTakesNone(t *testing.T) {
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			got := columnKeyPlan(c, dialect, "CREATE TABLE parent (id bigint NOT NULL);",
				"CREATE TABLE parent (id bigint NOT NULL, CONSTRAINT parent_pk PRIMARY KEY (id));")
			c.Assert(got, qt.DeepEquals, []string{"ALTER TABLE `parent` ADD PRIMARY KEY (`id`)"})
		})
	}
}
