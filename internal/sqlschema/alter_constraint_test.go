package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/sqlschema"
)

// deferralOf lists each foreign key the model holds as
// `<name>: deferrable=<bool> initially=<timing>`, whether the key is a
// constraint of the table or is written on a column.
func deferralOf(t *testing.T, sql string) []string {
	t.Helper()
	database, _, err := sqlschema.Read([]byte(sql), "postgres")
	qt.New(t).Assert(err, qt.IsNil)
	var described []string
	for _, constraint := range database.Constraints {
		if constraint.Type == "FOREIGN KEY" {
			described = append(described, constraint.Name+": deferrable="+boolWord(constraint.Deferrable)+" initially="+constraint.Initially)
		}
	}
	for _, field := range database.Fields {
		if field.Foreign != "" {
			described = append(described, field.ForeignKeyName+": deferrable="+boolWord(field.Deferrable)+" initially="+field.Initially)
		}
	}
	return described
}

func boolWord(value bool) string {
	return map[bool]string{true: "true", false: "false"}[value]
}

// TestRead_AlterConstraint_HappyPath reads ALTER CONSTRAINT onto a foreign key,
// the statement Ent's schema export writes to make its keys deferred. Each row's
// SQL was run on PostgreSQL 15.19, which recorded what the row expects in
// pg_constraint.condeferrable and condeferred.
func TestRead_AlterConstraint_HappyPath(t *testing.T) {
	const tables = "CREATE TABLE p (id int PRIMARY KEY);\nCREATE TABLE c (id int PRIMARY KEY, p int);\n" +
		"ALTER TABLE c ADD CONSTRAINT c_p FOREIGN KEY (p) REFERENCES p (id);\n"
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "deferrable and initially deferred",
			sql:  tables + "ALTER TABLE ONLY c ALTER CONSTRAINT c_p DEFERRABLE INITIALLY DEFERRED;",
			want: []string{"c_p: deferrable=true initially=deferred"},
		},
		{
			name: "deferrable alone keeps the immediate default",
			sql:  tables + "ALTER TABLE c ALTER CONSTRAINT c_p DEFERRABLE;",
			want: []string{"c_p: deferrable=true initially="},
		},
		{
			name: "a timing alone changes the timing of a deferrable key",
			sql:  tables + "ALTER TABLE c ALTER CONSTRAINT c_p DEFERRABLE INITIALLY IMMEDIATE;\nALTER TABLE c ALTER CONSTRAINT c_p INITIALLY DEFERRED;",
			want: []string{"c_p: deferrable=true initially=deferred"},
		},
		{
			name: "not deferrable clears the timing",
			sql:  tables + "ALTER TABLE c ALTER CONSTRAINT c_p DEFERRABLE INITIALLY DEFERRED;\nALTER TABLE c ALTER CONSTRAINT c_p NOT DEFERRABLE;",
			want: []string{"c_p: deferrable=false initially="},
		},
		{
			name: "a timing alone makes a key that was not deferrable deferrable",
			sql:  tables + "ALTER TABLE c ALTER CONSTRAINT c_p INITIALLY DEFERRED;",
			want: []string{"c_p: deferrable=true initially=deferred"},
		},
		{
			name: "INITIALLY IMMEDIATE alone leaves a key not deferrable",
			sql:  tables + "ALTER TABLE c ALTER CONSTRAINT c_p INITIALLY IMMEDIATE;",
			want: []string{"c_p: deferrable=false initially="},
		},
		{
			name: "a key written on a column",
			sql: "CREATE TABLE p (id int PRIMARY KEY);\nCREATE TABLE c (p int CONSTRAINT c_p REFERENCES p (id));\n" +
				"ALTER TABLE c ALTER CONSTRAINT c_p DEFERRABLE INITIALLY DEFERRED;",
			want: []string{"c_p: deferrable=true initially=deferred"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			qt.New(t).Assert(deferralOf(t, test.sql), qt.DeepEquals, test.want)
		})
	}
}

// TestRead_AlterConstraint_FailurePath refuses a name that is no foreign key, a
// statement that changes nothing the reader models, and a dialect with no
// deferral.
func TestRead_AlterConstraint_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		wantErr string
	}{
		{
			name:    "a name the table does not hold",
			dialect: "postgres",
			sql:     "CREATE TABLE c (a int);\nALTER TABLE c ALTER CONSTRAINT c_missing DEFERRABLE;",
			wantErr: `(?s).*ALTER TABLE c ALTER CONSTRAINT c_missing names no foreign key this schema declares by that name.*`,
		},
		{
			name:    "a UNIQUE constraint",
			dialect: "postgres",
			sql:     "CREATE TABLE c (a int, CONSTRAINT c_uq UNIQUE (a));\nALTER TABLE c ALTER CONSTRAINT c_uq DEFERRABLE;",
			wantErr: `(?s).*ALTER TABLE c ALTER CONSTRAINT c_uq names no foreign key this schema declares by that name.*`,
		},
		{
			name:    "no clause the reader models",
			dialect: "postgres",
			sql:     "CREATE TABLE c (a int);\nALTER TABLE c ALTER CONSTRAINT c_a;",
			wantErr: `(?s).*ALTER CONSTRAINT at position \d+: only \[NOT\] DEFERRABLE and INITIALLY DEFERRED \| IMMEDIATE are supported.*`,
		},
		{
			name:    "MySQL has no deferral",
			dialect: "mysql",
			sql:     "CREATE TABLE c (a int);\nALTER TABLE c ALTER CONSTRAINT c_a DEFERRABLE;",
			wantErr: `(?s).*deferral clause at position \d+.*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, _, err := sqlschema.Read([]byte(test.sql), test.dialect)

			qt.New(t).Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}
