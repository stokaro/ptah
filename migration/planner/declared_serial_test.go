package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestDeclaredSerial_HappyPath pins the plan for a plain column the declaration writes
// with a serial type. The comparison left a serial declaration alone, so `id bigint`
// against `id bigserial` read as synced, and a nullable `int` against `serial` planned
// DROP NOT NULL on every run.
func TestDeclaredSerial_HappyPath(t *testing.T) {
	tests := []struct {
		name, dialect, held, desired string
		want                         []string
	}{
		{
			name: "a key column", dialect: "postgres",
			held:    `CREATE TABLE parent (id bigint PRIMARY KEY);`,
			desired: `CREATE TABLE parent (id bigserial PRIMARY KEY);`,
			want: []string{
				`CREATE SEQUENCE "parent_id_seq" AS bigint`,
				`ALTER TABLE "parent" ALTER COLUMN "id" SET DEFAULT nextval('"parent_id_seq"')`,
				`SELECT setval('"parent_id_seq"', max("id")) FROM "parent" HAVING max("id") >= 1`,
				`ALTER SEQUENCE "parent_id_seq" OWNED BY "parent"."id"`,
			},
		},
		{
			name: "a nullable column takes NOT NULL", dialect: "postgres",
			held:    `CREATE TABLE parent (id bigint PRIMARY KEY, c int);`,
			desired: `CREATE TABLE parent (id bigint PRIMARY KEY, c serial);`,
			want: []string{
				`CREATE SEQUENCE "parent_c_seq" AS integer`,
				`UPDATE "parent" SET "c" = nextval('"parent_c_seq"') WHERE "c" IS NULL`,
				`ALTER TABLE "parent" ALTER COLUMN "c" SET NOT NULL`,
				`ALTER TABLE "parent" ALTER COLUMN "c" SET DEFAULT nextval('"parent_c_seq"')`,
				`SELECT setval('"parent_c_seq"', max("c")) FROM "parent" HAVING max("c") >= 1`,
				`ALTER SEQUENCE "parent_c_seq" OWNED BY "parent"."c"`,
			},
		},
		{
			name: "a plain default gives way", dialect: "postgres",
			held:    `CREATE TABLE parent (id bigint PRIMARY KEY, c smallint NOT NULL DEFAULT 5);`,
			desired: `CREATE TABLE parent (id bigint PRIMARY KEY, c smallserial);`,
			want: []string{
				`CREATE SEQUENCE "parent_c_seq" AS smallint`,
				`ALTER TABLE "parent" ALTER COLUMN "c" SET DEFAULT nextval('"parent_c_seq"')`,
				`SELECT setval('"parent_c_seq"', max("c")) FROM "parent" HAVING max("c") >= 1`,
				`ALTER SEQUENCE "parent_c_seq" OWNED BY "parent"."c"`,
			},
		},
		{
			name: "a sequence the column does not own gives way", dialect: "postgres",
			held:    `CREATE SEQUENCE other; CREATE TABLE parent (id bigint PRIMARY KEY DEFAULT nextval('other'));`,
			desired: `CREATE SEQUENCE other; CREATE TABLE parent (id bigserial PRIMARY KEY);`,
			want: []string{
				`CREATE SEQUENCE "parent_id_seq" AS bigint`,
				`ALTER TABLE "parent" ALTER COLUMN "id" SET DEFAULT nextval('"parent_id_seq"')`,
				`SELECT setval('"parent_id_seq"', max("id")) FROM "parent" HAVING max("id") >= 1`,
				`ALTER SEQUENCE "parent_id_seq" OWNED BY "parent"."id"`,
			},
		},
		{
			name: "CockroachDB takes the default it writes", dialect: "cockroachdb",
			held:    `CREATE TABLE parent (id bigint PRIMARY KEY);`,
			desired: `CREATE TABLE parent (id bigserial PRIMARY KEY);`,
			want:    []string{`ALTER TABLE "parent" ALTER COLUMN "id" SET DEFAULT unique_rowid()`},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := columnKeyPlan(c, test.dialect, test.held, test.desired, "id")

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestDeclaredSerial_FailurePath pins the pairs that plan nothing: the column is serial
// already, or the declaration says what it draws from.
func TestDeclaredSerial_FailurePath(t *testing.T) {
	const serial = `CREATE TABLE parent (id bigint PRIMARY KEY, c serial);`
	tests := []struct{ name, dialect, held, desired string }{
		{name: "serial against serial on PostgreSQL", dialect: "postgres", held: serial, desired: serial},
		{name: "serial against serial on CockroachDB", dialect: "cockroachdb", held: serial, desired: serial},
		{name: "a dialect that does not compare it", dialect: "yugabytedb", held: `CREATE TABLE parent (id bigint PRIMARY KEY);`, desired: `CREATE TABLE parent (id bigserial PRIMARY KEY);`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := columnKeyPlan(c, test.dialect, test.held, test.desired, "id")

			c.Assert(got, qt.HasLen, 0)
		})
	}
}
