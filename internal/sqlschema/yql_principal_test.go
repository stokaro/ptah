package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/coverage"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlschema"
)

func TestReadYQLPrincipals(t *testing.T) {
	for _, test := range []struct {
		name, sql string
		want      []schemamodel.Role
	}{
		{"defaults", "CREATE USER app; CREATE GROUP readers;", []schemamodel.Role{
			{Name: "app", Login: true, Inherit: true}, {Name: "readers", Group: true, Inherit: true},
		}},
		{"options", "CREATE USER `app` PASSWORD 'Secret1!' NOLOGIN; ALTER USER app WITH LOGIN;", []schemamodel.Role{
			{Name: "app", Password: "Secret1!", Login: true, Inherit: true},
		}},
		{"null", "CREATE USER app PASSWORD NULL;", []schemamodel.Role{{Name: "app", Login: true, Inherit: true}}},
		{"hash", `CREATE USER app HASH '{"hash":"abc","salt":"abc","type":"argon2id"}';`, []schemamodel.Role{
			// #nosec G101 -- Deliberately invalid fixture hash verifies decoding without real credentials.
			{Name: "app", Password: `{"hash":"abc","salt":"abc","type":"argon2id"}`, Login: true, Inherit: true},
		}},
		{"members", "CREATE USER app; CREATE GROUP readers WITH USER app; ALTER GROUP readers ADD USER app; ALTER GROUP `DATA-READERS` ADD USER app;", []schemamodel.Role{
			{Name: "app", Login: true, Inherit: true, MemberOf: []string{"readers", "DATA-READERS"}}, {Name: "readers", Group: true, Inherit: true},
		}},
		{"remove member", "CREATE USER app; CREATE GROUP readers WITH USER app; ALTER GROUP readers DROP USER app;", []schemamodel.Role{
			{Name: "app", Login: true, Inherit: true, MemberOf: make([]string, 0)}, {Name: "readers", Group: true, Inherit: true},
		}},
		{"group member", "CREATE USER app; CREATE GROUP readers; CREATE GROUP auditors WITH USER app, readers;", []schemamodel.Role{
			{Name: "app", Login: true, Inherit: true, MemberOf: []string{"auditors"}},
			{Name: "readers", Group: true, Inherit: true, MemberOf: []string{"auditors"}}, {Name: "auditors", Group: true, Inherit: true},
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database, _, err := sqlschema.Read([]byte(test.sql), "ydb")
			c.Assert(err, qt.IsNil)
			c.Assert(database.Roles, qt.DeepEquals, test.want)
			c.Assert(database.NotDescribed.Describes(coverage.Role), qt.IsTrue)
			c.Assert(database.NotDescribed.Describes(coverage.Grant), qt.IsTrue)
		})
	}
}

func TestReadYQLPrincipalRefusals(t *testing.T) {
	for _, sql := range []string{
		"CREATE USER app; CREATE GROUP app;",
		"CREATE GROUP readers; ALTER USER readers NOLOGIN;",
		"ALTER USER missing NOLOGIN;",
		"CREATE GROUP readers WITH USER missing;",
		"CREATE USER app; ALTER GROUP app ADD USER app;",
		"CREATE USER app; ALTER GROUP `DATA-READERS` DROP USER app;",
		"CREATE GROUP readers; ALTER GROUP readers ADD USER readers;",
		"CREATE USER app; ALTER USER app PASSWORD NULL;",
		"CREATE USER app; ALTER USER app PASSWORD '';",
		"CREATE USER app WITH LOGIN;", "CREATE USER \"app\";",
		"CREATE USER app LOGIN NOLOGIN;", "CREATE USER app PASSWORD 'x' PASSWORD 'y';",
		"CREATE USER app HASH 'bad';", "CREATE USER app PASSWORD 123;",
		"CREATE USER app PASSWORD 'password'u;", "CREATE USER App;",
		"CREATE USER IF NOT EXISTS app;", "ALTER USER app;",
		"CREATE GROUP readers WITH USER", "CREATE GROUP readers WITH USER app,",
		"CREATE GROUP readers WITH USER app,;", "CREATE GROUP readers LOGIN;",
	} {
		t.Run(sql, func(t *testing.T) {
			c := qt.New(t)
			database, statements, err := sqlschema.Read([]byte(sql), "ydb")
			c.Assert(err, qt.IsNotNil)
			c.Assert(database.Roles, qt.HasLen, 0)
			c.Assert(statements, qt.IsNil)
		})
	}
}

func TestReadYQLUserErrorHidesCredentials(t *testing.T) {
	for _, sql := range []string{
		"CREATE USER app PASSWORD 'SENTINEL' UNKNOWN;",
		"CREATE USER app HASH 'SENTINEL';",
		"CREATE USER app PASSWORD 'SENTINEL",
		"CREATE USER app PASSWORD SENTINEL;",
		"CREATE USER SENTINEL;",
		"ALTER USER app PASSWORD 'SENTINEL' UNKNOWN;",
		"ALTER USER missing PASSWORD 'SENTINEL';",
	} {
		t.Run(sql, func(t *testing.T) {
			c := qt.New(t)
			_, _, err := sqlschema.Read([]byte(sql), "ydb")
			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Not(qt.Contains), "SENTINEL")
		})
	}
}

func TestReadOntoYQLPrincipals(t *testing.T) {
	c := qt.New(t)
	earlier, _, err := sqlschema.Read([]byte("CREATE USER app; CREATE GROUP readers;"), "ydb")
	c.Assert(err, qt.IsNil)
	later, _, err := sqlschema.ReadOnto([]byte("ALTER USER app PASSWORD 'Secret2!' NOLOGIN; ALTER GROUP readers ADD USER app;"), "ydb", sqlschema.NewDocument(&earlier))
	c.Assert(err, qt.IsNil)
	c.Assert(later.Roles, qt.HasLen, 0)
	c.Assert(earlier.Roles[0].Login, qt.IsFalse)
	c.Assert(earlier.Roles[0].Password, qt.Equals, "Secret2!")
	c.Assert(earlier.Roles[0].MemberOf, qt.DeepEquals, []string{"readers"})
	_, _, err = sqlschema.ReadOnto([]byte("CREATE GROUP app;"), "ydb", sqlschema.NewDocument(&earlier))
	c.Assert(err, qt.ErrorIs, sqlschema.ErrUnmodeledStatement)
}
