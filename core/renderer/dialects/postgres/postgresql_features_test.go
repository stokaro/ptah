package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/renderer/dialects/postgres"
)

func TestPostgreSQLRenderer_VisitIndex_PostgreSQLFeatures(t *testing.T) {
	tests := []struct {
		name     string
		index    *ast.IndexNode
		expected string
	}{
		{
			name: "basic BTREE index",
			index: &ast.IndexNode{
				Name:    "idx_users_email",
				Table:   "users",
				Columns: []string{"email"},
				Unique:  false,
			},
			expected: "CREATE INDEX idx_users_email ON users (email);\n",
		},
		{
			name: "unique index",
			index: &ast.IndexNode{
				Name:    "idx_users_username",
				Table:   "users",
				Columns: []string{"username"},
				Unique:  true,
			},
			expected: "CREATE UNIQUE INDEX idx_users_username ON users (username);\n",
		},
		{
			name: "GIN index",
			index: &ast.IndexNode{
				Name:    "idx_products_tags",
				Table:   "products",
				Columns: []string{"tags"},
				Type:    "GIN",
			},
			expected: "CREATE INDEX idx_products_tags ON products USING GIN (tags);\n",
		},
		{
			name: "descending index part",
			index: ast.NewIndex("idx_users_rank", "users", "rank").
				SetParts([]ast.IndexPart{{Name: "rank", Desc: true}}),
			expected: "CREATE INDEX idx_users_rank ON users (rank DESC);\n",
		},
		{
			name: "expression index part",
			index: ast.NewIndex("idx_users_full_name", "users", "first_name || ' ' || last_name").
				SetParts([]ast.IndexPart{{Expr: "first_name || ' ' || last_name"}}),
			expected: "CREATE INDEX idx_users_full_name ON users ((first_name || ' ' || last_name));\n",
		},
		{
			name: "partial index",
			index: &ast.IndexNode{
				Name:      "idx_active_users",
				Table:     "users",
				Columns:   []string{"status"},
				Condition: "deleted_at IS NULL",
			},
			expected: "CREATE INDEX idx_active_users ON users (status) WHERE deleted_at IS NULL;\n",
		},
		{
			name: "covering index",
			index: &ast.IndexNode{
				Name:           "idx_users_name",
				Table:          "users",
				Columns:        []string{"name"},
				IncludeColumns: []string{"active"},
				Condition:      "active",
			},
			expected: "CREATE INDEX idx_users_name ON users (name) INCLUDE (active) WHERE active;\n",
		},
		{
			name: "BRIN index with storage params",
			index: &ast.IndexNode{
				Name:          "idx_users_c",
				Table:         "users",
				Columns:       []string{"c"},
				Type:          "BRIN",
				StorageParams: map[string]string{"pages_per_range": "2"},
			},
			expected: "CREATE INDEX idx_users_c ON users USING BRIN (c) WITH (pages_per_range='2');\n",
		},
		{
			name: "trigram index",
			index: &ast.IndexNode{
				Name:     "idx_users_name_trgm",
				Table:    "users",
				Columns:  []string{"name"},
				Type:     "GIN",
				Operator: "gin_trgm_ops",
			},
			expected: "CREATE INDEX idx_users_name_trgm ON users USING GIN (name gin_trgm_ops);\n",
		},
		{
			name: "composite GIN index with condition",
			index: &ast.IndexNode{
				Name:      "idx_products_search",
				Table:     "products",
				Columns:   []string{"name", "tags"},
				Type:      "GIN",
				Condition: "status = 'active'",
			},
			expected: "CREATE INDEX idx_products_search ON products USING GIN (name, tags) WHERE status = 'active';\n",
		},
		{
			name: "complex index with all features",
			index: &ast.IndexNode{
				Name:      "idx_complex",
				Table:     "products",
				Columns:   []string{"name", "description"},
				Type:      "GIN",
				Operator:  "gin_trgm_ops",
				Condition: "status = 'published' AND deleted_at IS NULL",
				Unique:    false,
			},
			expected: "CREATE INDEX idx_complex ON products USING GIN (name gin_trgm_ops, description gin_trgm_ops) WHERE status = 'published' AND deleted_at IS NULL;\n",
		},
		{
			name: "index with IF NOT EXISTS",
			index: &ast.IndexNode{
				Name:        "idx_users_email",
				Table:       "users",
				Columns:     []string{"email"},
				IfNotExists: true,
			},
			expected: "CREATE INDEX IF NOT EXISTS idx_users_email ON users (email);\n",
		},
		{
			name: "unique index with IF NOT EXISTS",
			index: &ast.IndexNode{
				Name:        "idx_users_username",
				Table:       "users",
				Columns:     []string{"username"},
				Unique:      true,
				IfNotExists: true,
			},
			expected: "CREATE UNIQUE INDEX IF NOT EXISTS idx_users_username ON users (username);\n",
		},
		{
			name: "complex index with IF NOT EXISTS",
			index: &ast.IndexNode{
				Name:        "idx_products_search",
				Table:       "products",
				Columns:     []string{"name", "tags"},
				Type:        "GIN",
				Condition:   "status = 'active'",
				IfNotExists: true,
			},
			expected: "CREATE INDEX IF NOT EXISTS idx_products_search ON products USING GIN (name, tags) WHERE status = 'active';\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			renderer := postgres.New()
			sql, err := renderer.Render(tt.index)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.expected)
		})
	}
}

func TestPostgreSQLRenderer_RejectsUnsafeIndexStorageParamName(t *testing.T) {
	c := qt.New(t)
	renderer := postgres.New()
	index := &ast.IndexNode{
		Name:          "idx_users_c",
		Table:         "users",
		Columns:       []string{"c"},
		Type:          "BRIN",
		StorageParams: map[string]string{"pages_per_range) = 2; DROP TABLE users; --": "2"},
	}

	_, err := renderer.Render(index)
	c.Assert(err, qt.ErrorMatches, `invalid PostgreSQL index storage parameter .*`)
}

func TestPostgreSQLRenderer_VisitExtension(t *testing.T) {
	tests := []struct {
		name      string
		extension *ast.ExtensionNode
		expected  string
	}{
		{
			name: "basic extension",
			extension: &ast.ExtensionNode{
				Name: "pg_trgm",
			},
			expected: "CREATE EXTENSION pg_trgm;\n",
		},
		{
			name: "extension with IF NOT EXISTS",
			extension: &ast.ExtensionNode{
				Name:        "pg_trgm",
				IfNotExists: true,
			},
			expected: "CREATE EXTENSION IF NOT EXISTS pg_trgm;\n",
		},
		{
			name: "extension with version",
			extension: &ast.ExtensionNode{
				Name:    "postgis",
				Version: "3.0",
			},
			expected: "CREATE EXTENSION postgis VERSION '3.0';\n",
		},
		{
			name: "extension with comment",
			extension: &ast.ExtensionNode{
				Name:    "btree_gin",
				Comment: "Enable GIN indexes on btree types",
			},
			expected: "-- Enable GIN indexes on btree types\nCREATE EXTENSION btree_gin;\n",
		},
		{
			name: "extension with all features",
			extension: &ast.ExtensionNode{
				Name:        "postgis",
				IfNotExists: true,
				Version:     "3.0",
				Comment:     "Geographic data support",
			},
			expected: "-- Geographic data support\nCREATE EXTENSION IF NOT EXISTS postgis VERSION '3.0';\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			renderer := postgres.New()
			sql, err := renderer.Render(tt.extension)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.expected)
		})
	}
}

func TestPostgreSQLRenderer_CompleteSchema(t *testing.T) {
	c := qt.New(t)

	// Create a complete schema with extensions, tables, and indexes
	renderer := postgres.New()

	// Extension
	extension := &ast.ExtensionNode{
		Name:        "pg_trgm",
		IfNotExists: true,
		Comment:     "Enable trigram similarity search",
	}

	// Table
	table := ast.NewCreateTable("products").
		AddColumn(
			ast.NewColumn("id", "SERIAL").
				SetPrimary().
				SetNotNull(),
		).
		AddColumn(
			ast.NewColumn("name", "VARCHAR(255)").
				SetNotNull(),
		).
		AddColumn(
			ast.NewColumn("tags", "JSONB"),
		).
		AddColumn(
			ast.NewColumn("status", "VARCHAR(50)"),
		).
		AddColumn(
			ast.NewColumn("deleted_at", "TIMESTAMP"),
		)

	// Indexes
	ginIndex := &ast.IndexNode{
		Name:    "idx_products_tags",
		Table:   "products",
		Columns: []string{"tags"},
		Type:    "GIN",
	}

	partialIndex := &ast.IndexNode{
		Name:      "idx_active_products",
		Table:     "products",
		Columns:   []string{"status"},
		Condition: "deleted_at IS NULL",
	}

	trigramIndex := &ast.IndexNode{
		Name:     "idx_products_name_trgm",
		Table:    "products",
		Columns:  []string{"name"},
		Type:     "GIN",
		Operator: "gin_trgm_ops",
	}

	// Render each component
	extensionSQL, err := renderer.Render(extension)
	c.Assert(err, qt.IsNil)

	tableSQL, err := renderer.Render(table)
	c.Assert(err, qt.IsNil)

	ginIndexSQL, err := renderer.Render(ginIndex)
	c.Assert(err, qt.IsNil)

	partialIndexSQL, err := renderer.Render(partialIndex)
	c.Assert(err, qt.IsNil)

	trigramIndexSQL, err := renderer.Render(trigramIndex)
	c.Assert(err, qt.IsNil)

	// Verify the generated SQL
	c.Assert(extensionSQL, qt.Contains, "CREATE EXTENSION IF NOT EXISTS pg_trgm")
	c.Assert(extensionSQL, qt.Contains, "-- Enable trigram similarity search")

	c.Assert(tableSQL, qt.Contains, "CREATE TABLE products")
	c.Assert(tableSQL, qt.Contains, "id SERIAL PRIMARY KEY NOT NULL")
	c.Assert(tableSQL, qt.Contains, "tags JSONB")

	c.Assert(ginIndexSQL, qt.Equals, "CREATE INDEX idx_products_tags ON products USING GIN (tags);\n")
	c.Assert(partialIndexSQL, qt.Equals, "CREATE INDEX idx_active_products ON products (status) WHERE deleted_at IS NULL;\n")
	c.Assert(trigramIndexSQL, qt.Equals, "CREATE INDEX idx_products_name_trgm ON products USING GIN (name gin_trgm_ops);\n")
}

func TestPostgreSQLRenderer_VisitDropExtension(t *testing.T) {
	tests := []struct {
		name     string
		dropExt  *ast.DropExtensionNode
		expected string
	}{
		{
			name: "basic drop extension",
			dropExt: &ast.DropExtensionNode{
				Name: "pg_trgm",
			},
			expected: "DROP EXTENSION pg_trgm;\n",
		},
		{
			name: "drop extension with IF EXISTS",
			dropExt: &ast.DropExtensionNode{
				Name:     "pg_trgm",
				IfExists: true,
			},
			expected: "DROP EXTENSION IF EXISTS pg_trgm;\n",
		},
		{
			name: "drop extension with CASCADE",
			dropExt: &ast.DropExtensionNode{
				Name:    "postgis",
				Cascade: true,
			},
			expected: "DROP EXTENSION postgis CASCADE;\n",
		},
		{
			name: "drop extension with comment",
			dropExt: &ast.DropExtensionNode{
				Name:    "btree_gin",
				Comment: "Remove unused extension",
			},
			expected: "-- Remove unused extension\nDROP EXTENSION btree_gin;\n",
		},
		{
			name: "drop extension with all features",
			dropExt: &ast.DropExtensionNode{
				Name:     "postgis",
				IfExists: true,
				Cascade:  true,
				Comment:  "Remove geographic data support",
			},
			expected: "-- Remove geographic data support\nDROP EXTENSION IF EXISTS postgis CASCADE;\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			renderer := postgres.New()
			sql, err := renderer.Render(tt.dropExt)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.expected)
		})
	}
}

func TestPostgreSQLRenderer_VisitCreateFunction(t *testing.T) {
	tests := []struct {
		name     string
		function *ast.CreateFunctionNode
		expected string
	}{
		{
			name: "basic function",
			function: ast.NewCreateFunction("set_tenant_context").
				SetParameters("tenant_id_param TEXT").
				SetReturns("VOID").
				SetLanguage("plpgsql").
				SetBody("BEGIN PERFORM set_config('app.current_tenant_id', tenant_id_param, false); END;"),
			expected: `CREATE OR REPLACE FUNCTION set_tenant_context(tenant_id_param TEXT) RETURNS VOID AS $$
BEGIN PERFORM set_config('app.current_tenant_id', tenant_id_param, false); END;
$$
LANGUAGE plpgsql;
`,
		},
		{
			name: "function with security and volatility",
			function: ast.NewCreateFunction("get_current_tenant_id").
				SetReturns("TEXT").
				SetLanguage("plpgsql").
				SetSecurity("DEFINER").
				SetVolatility("STABLE").
				SetBody("BEGIN RETURN current_setting('app.current_tenant_id', true); END;"),
			expected: `CREATE OR REPLACE FUNCTION get_current_tenant_id() RETURNS TEXT AS $$
BEGIN RETURN current_setting('app.current_tenant_id', true); END;
$$
LANGUAGE plpgsql SECURITY DEFINER STABLE;
`,
		},
		{
			name: "function with comment",
			function: ast.NewCreateFunction("test_function").
				SetReturns("INTEGER").
				SetLanguage("sql").
				SetBody("SELECT 42").
				SetComment("Test function for unit tests"),
			expected: `-- Test function for unit tests
CREATE OR REPLACE FUNCTION test_function() RETURNS INTEGER AS $$
SELECT 42
$$
LANGUAGE sql;
`,
		},
		{
			name: "function with return body",
			function: ast.NewCreateFunction("first_int").
				SetParameters("value text[]").
				SetReturns("int").
				SetLanguage("sql").
				SetReturnBody("value[1]::int"),
			expected: `CREATE OR REPLACE FUNCTION first_int(value text[]) RETURNS int LANGUAGE sql RETURN value[1]::int;
`,
		},
		{
			name: "function with atomic body",
			function: ast.NewCreateFunction("is_even").
				SetParameters("x int").
				SetReturns("boolean").
				SetLanguage("SQL").
				SetVolatility("STABLE").
				SetAtomicBody(`BEGIN ATOMIC
SELECT CASE WHEN x % 2 = 0 THEN true ELSE false END;
END`),
			expected: `CREATE OR REPLACE FUNCTION is_even(x int) RETURNS boolean LANGUAGE SQL STABLE BEGIN ATOMIC
SELECT CASE WHEN x % 2 = 0 THEN true ELSE false END;
END;
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			renderer := postgres.New()
			sql, err := renderer.Render(tt.function)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.expected)
		})
	}
}

func TestPostgreSQLRenderer_VisitCreatePolicy(t *testing.T) {
	tests := []struct {
		name     string
		policy   *ast.CreatePolicyNode
		expected string
	}{
		{
			name: "basic RLS policy",
			policy: ast.NewCreatePolicy("user_tenant_isolation", "users").
				SetPolicyFor("ALL").
				SetToRoles("inventario_app").
				SetUsingExpression("tenant_id = get_current_tenant_id()"),
			expected: `CREATE POLICY user_tenant_isolation ON users FOR ALL TO inventario_app
    USING (tenant_id = get_current_tenant_id())
;
`,
		},
		{
			name: "policy with WITH CHECK",
			policy: ast.NewCreatePolicy("insert_policy", "products").
				SetPolicyFor("INSERT").
				SetToRoles("app_user").
				SetUsingExpression("tenant_id = get_current_tenant_id()").
				SetWithCheckExpression("tenant_id = get_current_tenant_id()"),
			expected: `CREATE POLICY insert_policy ON products FOR INSERT TO app_user
    USING (tenant_id = get_current_tenant_id())
    WITH CHECK (tenant_id = get_current_tenant_id())
;
`,
		},
		{
			name: "policy with comment",
			policy: ast.NewCreatePolicy("select_policy", "orders").
				SetPolicyFor("SELECT").
				SetToRoles("PUBLIC").
				SetUsingExpression("user_id = current_user_id()").
				SetComment("Allow users to see only their orders"),
			expected: `-- Allow users to see only their orders
CREATE POLICY select_policy ON orders FOR SELECT TO PUBLIC
    USING (user_id = current_user_id())
;
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			renderer := postgres.New()
			sql, err := renderer.Render(tt.policy)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.expected)
		})
	}
}

func TestPostgreSQLRenderer_VisitAlterTableEnableRLS(t *testing.T) {
	tests := []struct {
		name      string
		enableRLS *ast.AlterTableEnableRLSNode
		expected  string
	}{
		{
			name:      "basic RLS enable",
			enableRLS: ast.NewAlterTableEnableRLS("users"),
			expected:  "ALTER TABLE users ENABLE ROW LEVEL SECURITY;\n",
		},
		{
			name: "RLS enable with comment",
			enableRLS: ast.NewAlterTableEnableRLS("products").
				SetComment("Enable RLS for multi-tenant isolation"),
			expected: `-- Enable RLS for multi-tenant isolation
ALTER TABLE products ENABLE ROW LEVEL SECURITY;
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			renderer := postgres.New()
			sql, err := renderer.Render(tt.enableRLS)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.expected)
		})
	}
}

func TestPostgreSQLRenderer_ExcludeConstraints(t *testing.T) {
	tests := []struct {
		name     string
		table    *ast.CreateTableNode
		expected string
	}{
		{
			name: "basic EXCLUDE constraint with GIST",
			table: ast.NewCreateTable("user_sessions").
				AddColumn(ast.NewColumn("user_id", "BIGINT").SetNotNull()).
				AddColumn(ast.NewColumn("is_active", "BOOLEAN").SetNotNull()).
				AddConstraint(ast.NewExcludeConstraint("one_active_session_per_user", "gist", "user_id WITH =").
					SetWhereCondition("is_active = true")),
			expected: `-- POSTGRES TABLE: user_sessions --
CREATE TABLE user_sessions (
  user_id BIGINT NOT NULL,
  is_active BOOLEAN NOT NULL,
  CONSTRAINT one_active_session_per_user EXCLUDE USING gist (user_id WITH =) WHERE (is_active = true)
);

`,
		},
		{
			name: "EXCLUDE constraint without WHERE clause",
			table: ast.NewCreateTable("bookings").
				AddColumn(ast.NewColumn("room_id", "INTEGER").SetNotNull()).
				AddColumn(ast.NewColumn("during", "TSRANGE").SetNotNull()).
				AddConstraint(ast.NewExcludeConstraint("no_overlapping_bookings", "gist", "room_id WITH =, during WITH &&")),
			expected: `-- POSTGRES TABLE: bookings --
CREATE TABLE bookings (
  room_id INTEGER NOT NULL,
  during TSRANGE NOT NULL,
  CONSTRAINT no_overlapping_bookings EXCLUDE USING gist (room_id WITH =, during WITH &&)
);

`,
		},
		{
			name: "EXCLUDE constraint with BTREE method",
			table: ast.NewCreateTable("unique_values").
				AddColumn(ast.NewColumn("value", "INTEGER").SetNotNull()).
				AddConstraint(ast.NewExcludeConstraint("unique_values_constraint", "btree", "value WITH =")),
			expected: `-- POSTGRES TABLE: unique_values --
CREATE TABLE unique_values (
  value INTEGER NOT NULL,
  CONSTRAINT unique_values_constraint EXCLUDE USING btree (value WITH =)
);

`,
		},
		{
			name: "EXCLUDE constraint without name",
			table: ast.NewCreateTable("spatial_data").
				AddColumn(ast.NewColumn("location", "GEOMETRY").SetNotNull()).
				AddConstraint(&ast.ConstraintNode{
					Type:            ast.ExcludeConstraint,
					UsingMethod:     "gist",
					ExcludeElements: "location WITH &&",
				}),
			expected: `-- POSTGRES TABLE: spatial_data --
CREATE TABLE spatial_data (
  location GEOMETRY NOT NULL,
  EXCLUDE USING gist (location WITH &&)
);

`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			renderer := postgres.New()
			result, err := renderer.Render(tt.table)

			c.Assert(err, qt.IsNil)
			c.Assert(result, qt.Equals, tt.expected)
		})
	}
}

func TestPostgreSQLRenderer_IdentityColumns(t *testing.T) {
	c := qt.New(t)

	table := ast.NewCreateTable("users").
		AddColumn(ast.NewColumn("id", "INTEGER").SetNotNull().SetIdentity("ALWAYS", "10", "5")).
		AddColumn(ast.NewColumn("tenant_id", "BIGINT").SetIdentity("BY_DEFAULT", "", "")).
		AddColumn(ast.NewColumn("seq_id", "BIGINT").
			SetIdentity("BY_DEFAULT", "0", "-1").
			SetIdentityOptions("MINVALUE -100 MAXVALUE 0 START WITH 0 INCREMENT BY -1 CACHE 10 NO CYCLE"))

	renderer := postgres.New()
	result, err := renderer.Render(table)

	c.Assert(err, qt.IsNil)
	c.Assert(result, qt.Equals, `-- POSTGRES TABLE: users --
CREATE TABLE users (
  id INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 10 INCREMENT BY 5),
  tenant_id BIGINT GENERATED BY DEFAULT AS IDENTITY,
  seq_id BIGINT GENERATED BY DEFAULT AS IDENTITY (MINVALUE -100 MAXVALUE 0 START WITH 0 INCREMENT BY -1 CACHE 10 NO CYCLE)
);

`)
}

func TestPostgreSQLRenderer_PrimaryKeyInclude(t *testing.T) {
	c := qt.New(t)

	table := ast.NewCreateTable("users").
		AddColumn(ast.NewColumn("id", "INTEGER").SetNotNull()).
		AddColumn(ast.NewColumn("covering", "INTEGER")).
		AddConstraint(&ast.ConstraintNode{
			Type:           ast.PrimaryKeyConstraint,
			Name:           "users_pkey",
			Columns:        []string{"id"},
			IncludeColumns: []string{"covering"},
		})

	renderer := postgres.New()
	result, err := renderer.Render(table)

	c.Assert(err, qt.IsNil)
	c.Assert(result, qt.Contains, "  CONSTRAINT users_pkey PRIMARY KEY (id) INCLUDE (covering)")
}

func TestPostgreSQLRenderer_RejectsIdentityGeneratedColumnMix(t *testing.T) {
	c := qt.New(t)

	table := ast.NewCreateTable("users").
		AddColumn(ast.NewColumn("id", "INTEGER").
			SetIdentity("ALWAYS", "", "").
			SetGenerated("lower(name)", "STORED"))

	renderer := postgres.New()
	_, err := renderer.Render(table)

	c.Assert(err, qt.ErrorMatches, `.*postgres column id cannot be both identity and generated`)
}

func TestPostgreSQLRenderer_ExcludeConstraint_InCreateTable(t *testing.T) {
	c := qt.New(t)

	table := ast.NewCreateTable("user_sessions").
		AddColumn(ast.NewColumn("user_id", "BIGINT").SetNotNull()).
		AddColumn(ast.NewColumn("is_active", "BOOLEAN").SetNotNull().SetDefault("false")).
		AddConstraint(ast.NewExcludeConstraint("one_active_session_per_user", "gist", "user_id WITH =").
			SetWhereCondition("is_active = true"))

	renderer := postgres.New()
	result, err := renderer.Render(table)

	c.Assert(err, qt.IsNil)
	expected := `-- POSTGRES TABLE: user_sessions --
CREATE TABLE user_sessions (
  user_id BIGINT NOT NULL,
  is_active BOOLEAN NOT NULL DEFAULT 'false',
  CONSTRAINT one_active_session_per_user EXCLUDE USING gist (user_id WITH =) WHERE (is_active = true)
);

`
	c.Assert(result, qt.Equals, expected)
}

func TestPostgreSQLRenderer_ExcludeConstraint_Errors(t *testing.T) {
	tests := []struct {
		name  string
		table *ast.CreateTableNode
	}{
		{
			name: "missing using method",
			table: ast.NewCreateTable("test_table").
				AddColumn(ast.NewColumn("id", "INTEGER")).
				AddConstraint(&ast.ConstraintNode{
					Type:            ast.ExcludeConstraint,
					Name:            "test_exclude",
					ExcludeElements: "user_id WITH =",
				}),
		},
		{
			name: "missing exclude elements",
			table: ast.NewCreateTable("test_table").
				AddColumn(ast.NewColumn("id", "INTEGER")).
				AddConstraint(&ast.ConstraintNode{
					Type:        ast.ExcludeConstraint,
					Name:        "test_exclude",
					UsingMethod: "gist",
				}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			renderer := postgres.New()
			_, err := renderer.Render(tt.table)
			c.Assert(err, qt.IsNotNil)
		})
	}
}
