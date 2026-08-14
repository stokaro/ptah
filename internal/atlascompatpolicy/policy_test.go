package atlascompatpolicy_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlascompatpolicy"
	"go.5x5.cz/ptah/internal/envbool/envbooltest"
)

func TestResolveDefaultsToFullCompatibility(t *testing.T) {
	c := qt.New(t)
	envbooltest.Unset(atlascompatpolicy.StrictCompatEnvVar)(t)

	policy, err := atlascompatpolicy.Resolve()

	c.Assert(err, qt.IsNil)
	c.Assert(policy.IsStrictCE(), qt.IsFalse)
}

func TestResolveSelectsStrictCECompatibility(t *testing.T) {
	c := qt.New(t)
	t.Setenv(atlascompatpolicy.StrictCompatEnvVar, "true")

	policy, err := atlascompatpolicy.Resolve()

	c.Assert(err, qt.IsNil)
	c.Assert(policy.IsStrictCE(), qt.IsTrue)
}

func TestResolveRejectsMalformedStrictSelector(t *testing.T) {
	c := qt.New(t)
	t.Setenv(atlascompatpolicy.StrictCompatEnvVar, "")

	_, err := atlascompatpolicy.Resolve()

	c.Assert(err, qt.ErrorMatches,
		`invalid boolean value "" for PTAH_ATLAS_STRICT_COMPAT`)
}

func TestResolveStrictCERejectsEnabledExtensionEnvironment(t *testing.T) {
	for _, name := range []string{"PTAH_ATLAS_INSPECT_ALL_BLOCKS", "PTAH_SKIP_CHECKS"} {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			t.Setenv(atlascompatpolicy.StrictCompatEnvVar, "1")
			t.Setenv(name, "true")

			_, err := atlascompatpolicy.Resolve()

			c.Assert(err, qt.ErrorMatches,
				`PTAH_ATLAS_STRICT_COMPAT does not allow `+name)
		})
	}
}

func TestResolveStrictCEAllowsExplicitlyDisabledExtensionEnvironment(t *testing.T) {
	c := qt.New(t)
	t.Setenv(atlascompatpolicy.StrictCompatEnvVar, "1")
	t.Setenv("PTAH_ATLAS_INSPECT_ALL_BLOCKS", "false")

	policy, err := atlascompatpolicy.Resolve()

	c.Assert(err, qt.IsNil)
	c.Assert(policy.IsStrictCE(), qt.IsTrue)
}

func TestResolveStrictCERejectsMalformedExtensionEnvironment(t *testing.T) {
	c := qt.New(t)
	t.Setenv(atlascompatpolicy.StrictCompatEnvVar, "1")
	t.Setenv("PTAH_ATLAS_INSPECT_ALL_BLOCKS", "yes")

	_, err := atlascompatpolicy.Resolve()

	c.Assert(err, qt.ErrorMatches,
		`invalid boolean value "yes" for PTAH_ATLAS_INSPECT_ALL_BLOCKS`)
}

func TestResolveStrictCERejectsPtahLogEnvironment(t *testing.T) {
	c := qt.New(t)
	t.Setenv(atlascompatpolicy.StrictCompatEnvVar, "1")
	t.Setenv("PTAH_LOG_FORMAT", "")

	_, err := atlascompatpolicy.Resolve()

	c.Assert(err, qt.ErrorMatches,
		`PTAH_ATLAS_STRICT_COMPAT does not allow PTAH_LOG_FORMAT`)
}

func TestResolveFullCompatibilityDoesNotInspectExtensionEnvironment(t *testing.T) {
	c := qt.New(t)
	t.Setenv(atlascompatpolicy.StrictCompatEnvVar, "false")
	t.Setenv("PTAH_ATLAS_INSPECT_ALL_BLOCKS", "yes")

	policy, err := atlascompatpolicy.Resolve()

	c.Assert(err, qt.IsNil)
	c.Assert(policy.IsStrictCE(), qt.IsFalse)
}

func TestResolveStrictCEAllowsAtlasProjectInputEnvironment(t *testing.T) {
	c := qt.New(t)
	t.Setenv(atlascompatpolicy.StrictCompatEnvVar, "1")
	t.Setenv("PTAH_ATLAS_PROJECT_CONFIG_E2E_URL", "sqlite://project.db")

	policy, err := atlascompatpolicy.Resolve()

	c.Assert(err, qt.IsNil)
	c.Assert(policy.IsStrictCE(), qt.IsTrue)
}

func TestStrictCEValidatesBoundFlagEnvironment(t *testing.T) {
	c := qt.New(t)
	policy := atlascompatpolicy.StrictCE()

	c.Assert(policy.ValidateFlagEnvironment("PTAH_URL", "sqlite://db", "string"),
		qt.ErrorMatches, `PTAH_ATLAS_STRICT_COMPAT does not allow PTAH_URL`)
	c.Assert(policy.ValidateFlagEnvironment("PTAH_DRY_RUN", "true", "bool"),
		qt.ErrorMatches, `PTAH_ATLAS_STRICT_COMPAT does not allow PTAH_DRY_RUN`)
	c.Assert(policy.ValidateFlagEnvironment("PTAH_DRY_RUN", "false", "bool"), qt.IsNil)
	c.Assert(policy.ValidateFlagEnvironment("PTAH_DRY_RUN", "", "bool"),
		qt.ErrorMatches, `invalid boolean value "" for PTAH_DRY_RUN`)
}

func TestResolveStrictCERetainsSafetyEnvironment(t *testing.T) {
	c := qt.New(t)
	t.Setenv(atlascompatpolicy.StrictCompatEnvVar, "1")
	t.Setenv("PTAH_ATLAS_ALLOW_UNMATCHED_EXCLUDE", "1")
	t.Setenv("PTAH_HCL_STRICT_REDECLARATIONS", "1")
	t.Setenv("PTAH_STRICT_DIR_QUERY", "1")
	t.Setenv("PTAH_ALLOW_NONINTERACTIVE_EDIT", "1")
	// Retained rather than gated: a true spelling restores the DROP TABLE the
	// pinned community binary plans for a SQLite virtual table anyway, so it
	// adds no Atlas capability for strict mode to refuse.
	t.Setenv("PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP", "1")

	policy, err := atlascompatpolicy.Resolve()

	c.Assert(err, qt.IsNil)
	c.Assert(policy.IsStrictCE(), qt.IsTrue)
}

func TestResolveStrictCERejectsMalformedRetainedEnvironment(t *testing.T) {
	for _, name := range []string{
		"PTAH_ALLOW_NONINTERACTIVE_EDIT",
		"PTAH_ATLAS_ALLOW_UNMATCHED_EXCLUDE",
		"PTAH_HCL_STRICT_REDECLARATIONS",
		"PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP",
		"PTAH_STRICT_DIR_QUERY",
	} {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			t.Setenv(atlascompatpolicy.StrictCompatEnvVar, "1")
			t.Setenv(name, "maybe")

			_, err := atlascompatpolicy.Resolve()

			c.Assert(err, qt.ErrorMatches,
				`invalid boolean value "maybe" for `+name)
		})
	}
}

func TestStrictCEValidatesDesiredSchemaExtensions(t *testing.T) {
	tests := []struct {
		name     string
		database *goschema.Database
	}{
		{name: "extensions", database: &goschema.Database{Extensions: []goschema.Extension{{
			Name: "pgcrypto", Schema: "extensions",
		}}}},
		{name: "functions", database: &goschema.Database{Functions: []goschema.Function{{}}}},
		{name: "standalone sequences", database: &goschema.Database{Sequences: []goschema.Sequence{{}}}},
		{name: "domains", database: &goschema.Database{Domains: []goschema.Domain{{}}}},
		{name: "composite types", database: &goschema.Database{CompositeTypes: []goschema.CompositeType{{}}}},
		{name: "range types", database: &goschema.Database{Ranges: []goschema.Range{{}}}},
		{name: "views", database: &goschema.Database{Views: []goschema.View{{}}}},
		{name: "materialized views", database: &goschema.Database{MaterializedViews: []goschema.MaterializedView{{}}}},
		{name: "triggers", database: &goschema.Database{Triggers: []goschema.Trigger{{}}}},
		{name: "row-level security policies", database: &goschema.Database{RLSPolicies: []goschema.RLSPolicy{{}}}},
		{name: "row-level security settings", database: &goschema.Database{RLSEnabledTables: []goschema.RLSEnabledTable{{}}}},
		{name: "roles", database: &goschema.Database{Roles: []goschema.Role{{}}}},
		{name: "grants", database: &goschema.Database{Grants: []goschema.Grant{{}}}},
		{name: "managed data", database: &goschema.Database{ManagedData: []goschema.ManagedData{{}}}},
		{name: "table partitioning", database: &goschema.Database{Tables: []goschema.Table{{Partition: &goschema.PartitionSpec{}}}}},
		{name: "platform overrides", database: &goschema.Database{Fields: []goschema.Field{{Overrides: map[string]map[string]string{"mysql": {"type": "bigint"}}}}}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := atlascompatpolicy.StrictCE().ValidateDesiredSchema(test.database)

			c.Assert(err, qt.ErrorMatches,
				`Atlas Community Edition strict compatibility does not support desired schema `+test.name)
		})
	}
}

func TestStrictCEValidatesInspectedSchemaExtensions(t *testing.T) {
	c := qt.New(t)
	err := atlascompatpolicy.StrictCE().ValidateInspectedSchema(&goschema.Database{
		Views: []goschema.View{{Name: "active_users"}},
	})

	c.Assert(err, qt.ErrorMatches,
		`Atlas Community Edition strict compatibility does not support inspected schema views`)
	c.Assert(atlascompatpolicy.Full().ValidateInspectedSchema(&goschema.Database{
		Views: []goschema.View{{Name: "active_users"}},
	}),
		qt.IsNil,
	)
}

func TestStrictCEIgnoresOnlyInspectedSystemPlpgsqlExtension(t *testing.T) {
	c := qt.New(t)
	policy := atlascompatpolicy.StrictCE()
	c.Assert(policy.ValidateInspectedSchema(&goschema.Database{
		Extensions: []goschema.Extension{{Name: "plpgsql"}},
	}), qt.IsNil)
	c.Assert(policy.ValidateSchemaCleanSnapshot(&goschema.Database{
		Extensions: []goschema.Extension{{Name: "plpgsql"}},
	}), qt.IsNil)

	err := policy.ValidateInspectedSchema(&goschema.Database{
		Extensions: []goschema.Extension{{Name: "plpgsql"}, {Name: "citext"}},
	})
	c.Assert(err, qt.ErrorMatches,
		`Atlas Community Edition strict compatibility does not support inspected schema extensions`)

	err = policy.ValidateDesiredSchema(&goschema.Database{
		Extensions: []goschema.Extension{{Name: "plpgsql"}},
	})
	c.Assert(err, qt.ErrorMatches,
		`Atlas Community Edition strict compatibility does not support desired schema extensions`)
}

func TestStrictCEIgnoresOnlyInspectedPostgresPublicUsageBaseline(t *testing.T) {
	c := qt.New(t)
	policy := atlascompatpolicy.StrictCE()
	baseline := goschema.Grant{
		Role:       "PUBLIC",
		Privileges: []string{"USAGE"},
		OnSchema:   "public",
		GrantedBy:  "database_owner",
	}
	c.Assert(policy.ValidateInspectedSchema(&goschema.Database{
		Grants: []goschema.Grant{baseline},
	}), qt.IsNil)
	c.Assert(policy.ValidateSchemaCleanSnapshot(&goschema.Database{
		Grants: []goschema.Grant{baseline},
	}), qt.IsNil)

	for _, grant := range []goschema.Grant{
		{Role: "app", Privileges: []string{"USAGE"}, OnSchema: "public"},
		{Role: "PUBLIC", Privileges: []string{"CREATE"}, OnSchema: "public"},
		{Role: "PUBLIC", Privileges: []string{"USAGE"}, OnSchema: "app"},
		{Role: "PUBLIC", Privileges: []string{"USAGE"}, OnSchema: "public", WithOption: true},
	} {
		err := policy.ValidateInspectedSchema(&goschema.Database{
			Grants: []goschema.Grant{grant},
		})
		c.Assert(err, qt.ErrorMatches,
			`Atlas Community Edition strict compatibility does not support inspected schema grants`)
		err = policy.ValidateSchemaCleanSnapshot(&goschema.Database{
			Grants: []goschema.Grant{grant},
		})
		c.Assert(err, qt.ErrorMatches,
			`Atlas Community Edition strict compatibility does not support cleaning live schema grants`)
	}

	err := policy.ValidateDesiredSchema(&goschema.Database{
		Grants: []goschema.Grant{baseline},
	})
	c.Assert(err, qt.ErrorMatches,
		`Atlas Community Edition strict compatibility does not support desired schema grants`)
}

func TestPrepareInspectedSchemaRemovesOnlyStrictPostgresBaselines(t *testing.T) {
	c := qt.New(t)
	baseline := &dbschematypes.DBSchema{
		Extensions: []dbschematypes.DBExtension{{Name: "plpgsql"}},
		Grants: []dbschematypes.DBGrant{{
			Role:       "PUBLIC",
			Privilege:  "USAGE",
			ObjectType: "SCHEMA",
			ObjectName: "public",
			GrantedBy:  "database_owner",
		}},
	}

	prepared, err := atlascompatpolicy.StrictCE().PrepareInspectedSchema(baseline)
	c.Assert(err, qt.IsNil)
	c.Assert(prepared, qt.Not(qt.Equals), baseline)
	c.Assert(prepared.Extensions, qt.HasLen, 0)
	c.Assert(prepared.Grants, qt.HasLen, 0)
	c.Assert(baseline.Extensions, qt.HasLen, 1)
	c.Assert(baseline.Grants, qt.HasLen, 1)

	full, err := atlascompatpolicy.Full().PrepareInspectedSchema(baseline)
	c.Assert(err, qt.IsNil)
	c.Assert(full, qt.Equals, baseline)
	c.Assert(full.Extensions, qt.HasLen, 1)
	c.Assert(full.Grants, qt.HasLen, 1)
}

func TestFullCompatibilityRetainsDesiredSchemaExtensions(t *testing.T) {
	c := qt.New(t)
	database := &goschema.Database{
		Extensions: []goschema.Extension{{Name: "citext"}},
		Tables: []goschema.Table{{
			Partition: &goschema.PartitionSpec{Type: "RANGE"},
		}},
		Fields: []goschema.Field{{
			Overrides: map[string]map[string]string{"mysql": {"type": "bigint"}},
		}},
	}

	err := atlascompatpolicy.Full().ValidateDesiredSchema(database)

	c.Assert(err, qt.IsNil)
}

func TestStrictCEValidatesLiveSchemaCleanObjects(t *testing.T) {
	c := qt.New(t)
	policy := atlascompatpolicy.StrictCE()
	for _, kind := range []string{"table", "foreign_key", "enum"} {
		t.Run("accepts "+kind, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(policy.ValidateSchemaCleanObject(atlascompatpolicy.LiveSchemaObject{
				Kind: kind,
				Name: "kept",
			}), qt.IsNil)
		})
	}
	for _, kind := range []string{
		"aggregate",
		"collation",
		"composite",
		"default_privilege",
		"domain",
		"event",
		"foreign_table",
		"function",
		"materialized_view",
		"procedure",
		"range",
		"sequence",
		"view",
	} {
		t.Run("refuses "+kind, func(t *testing.T) {
			c := qt.New(t)
			err := policy.ValidateSchemaCleanObject(atlascompatpolicy.LiveSchemaObject{
				Kind: kind,
				Name: "pro_object",
			})

			c.Assert(err, qt.ErrorMatches,
				`Atlas Community Edition strict compatibility does not support cleaning live schema `+
					kind+` "pro_object"`)
		})
	}
	c.Assert(atlascompatpolicy.Full().ValidateSchemaCleanObject(atlascompatpolicy.LiveSchemaObject{
		Kind: "view",
		Name: "retained_by_full_mode",
	}),
		qt.IsNil,
	)
}

func TestStrictCEAcceptsImplicitSchemaCleanSequence(t *testing.T) {
	c := qt.New(t)
	c.Assert(atlascompatpolicy.StrictCE().ValidateSchemaCleanObject(atlascompatpolicy.LiveSchemaObject{
		Kind:             "sequence",
		Name:             "users_id_seq",
		ImplicitSequence: true,
	}),
		qt.IsNil,
	)
}

func TestStrictCEValidatesUnmodeledLiveSchemaInspectObjects(t *testing.T) {
	c := qt.New(t)
	policy := atlascompatpolicy.StrictCE()

	c.Assert(policy.ValidateLiveSchemaObject(atlascompatpolicy.LiveSchemaObject{
		Kind:             "sequence",
		Name:             "users_id_seq",
		ImplicitSequence: true,
	}), qt.IsNil)
	err := policy.ValidateLiveSchemaObject(atlascompatpolicy.LiveSchemaObject{
		Kind: "procedure",
		Name: "refresh_users()",
	})

	c.Assert(err, qt.ErrorMatches,
		`Atlas Community Edition strict compatibility does not support inspecting live schema procedure "refresh_users\(\)"`)
	c.Assert(atlascompatpolicy.Full().ValidateLiveSchemaObject(atlascompatpolicy.LiveSchemaObject{
		Kind: "procedure",
		Name: "retained_by_full_mode()",
	}),
		qt.IsNil,
	)
}

func TestStrictCEValidatesUnlistedLiveSchemaCleanObjects(t *testing.T) {
	c := qt.New(t)
	database := &goschema.Database{Triggers: []goschema.Trigger{{Name: "users_audit"}}}

	err := atlascompatpolicy.StrictCE().ValidateSchemaCleanSnapshot(database)

	c.Assert(err, qt.ErrorMatches,
		`Atlas Community Edition strict compatibility does not support cleaning live schema triggers`)
	c.Assert(atlascompatpolicy.Full().ValidateSchemaCleanSnapshot(database), qt.IsNil)
}

func TestStrictCEUnknownHCLPolicy(t *testing.T) {
	c := qt.New(t)
	c.Assert(atlascompatpolicy.StrictCE().IgnoreUnknownHCLNames(), qt.IsFalse)
	c.Assert(atlascompatpolicy.Full().IgnoreUnknownHCLNames(), qt.IsTrue)
}

func TestStrictCERejectsProSchemaInspectTemplateFunctions(t *testing.T) {
	for _, name := range []string{"hcl", "split", "write"} {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			err := atlascompatpolicy.StrictCE().ValidateSchemaInspectFormat(
				`{{ sql . | ` + name + ` }}`,
			)

			c.Assert(err, qt.ErrorMatches,
				`Atlas Community Edition strict compatibility does not support schema inspect template function "`+name+`"`)
		})
	}
}

func TestSchemaInspectTemplatePolicyPreservesLiteralsAndFullHelpers(t *testing.T) {
	c := qt.New(t)
	c.Assert(atlascompatpolicy.StrictCE().ValidateSchemaInspectFormat(`{{ "hcl split write" }}`),
		qt.IsNil,
	)
	c.Assert(atlascompatpolicy.Full().ValidateSchemaInspectFormat(`{{ hcl . | split | write "out" }}`),
		qt.IsNil,
	)
}

func TestStrictCERefusesIgnoredProjectConfigConstructs(t *testing.T) {
	c := qt.New(t)
	config := projectconfig.Config{IgnoredConstructs: []projectconfig.IgnoredAtlasConstruct{{
		Name:     "pro_option",
		Kind:     "attribute",
		Filename: "atlas.hcl",
		Line:     7,
	}}}

	err := atlascompatpolicy.StrictCE().ValidateProjectConfig(config)

	c.Assert(err, qt.ErrorMatches,
		`Atlas Community Edition strict compatibility refuses ignored atlas.hcl attribute "pro_option" at atlas.hcl:7`)
	c.Assert(atlascompatpolicy.Full().ValidateProjectConfig(config), qt.IsNil)
}

func TestStrictCERefusesUnenforceableSchemaApplyLintPolicy(t *testing.T) {
	c := qt.New(t)
	config := projectconfig.Config{Lint: projectconfig.LintConfig{
		RuleConfigs: map[string]projectconfig.LintRuleConfig{
			"DS": {Severity: "error"},
		},
	}}

	err := atlascompatpolicy.StrictCE().ValidateSchemaApplyConfig(config)

	c.Assert(err, qt.ErrorMatches,
		`Atlas Community Edition strict compatibility cannot enforce atlas.hcl lint policy during schema apply`)
	c.Assert(atlascompatpolicy.Full().ValidateSchemaApplyConfig(config), qt.IsNil)
	c.Assert(atlascompatpolicy.StrictCE().ValidateSchemaApplyConfig(projectconfig.Config{}),
		qt.IsNil,
	)
}

func TestStrictCEDatabaseDialectPolicy(t *testing.T) {
	for _, rawURL := range []string{
		"clickhouse://localhost/app",
		"cockroachdb://localhost/app",
		"yugabytedb://localhost/app",
		"sqlserver://localhost/app",
		"spanner://localhost/app",
	} {
		t.Run(rawURL, func(t *testing.T) {
			c := qt.New(t)
			err := atlascompatpolicy.StrictCE().ValidateURL(rawURL)

			c.Assert(err, qt.ErrorMatches,
				`Atlas Community Edition strict compatibility does not support database dialect ".+"`)
			c.Assert(atlascompatpolicy.Full().ValidateURL(rawURL), qt.IsNil)
		})
	}

	for _, rawURL := range []string{
		"postgres://localhost/app",
		"mysql://localhost/app",
		"mariadb://localhost/app",
		"sqlite://app.db",
		"file://schema.hcl",
		"env://schema.src",
	} {
		t.Run(rawURL, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(atlascompatpolicy.StrictCE().ValidateURL(rawURL), qt.IsNil)
		})
	}
}

func TestStrictCEValidatesProjectConfigDatabaseURLs(t *testing.T) {
	c := qt.New(t)
	err := atlascompatpolicy.StrictCE().ValidateProjectConfig(projectconfig.Config{
		DevURL: "clickhouse://localhost/dev",
	})

	c.Assert(err, qt.ErrorMatches,
		`Atlas Community Edition strict compatibility does not support database dialect "clickhouse"`)
}

func TestStrictCEValidatesLocalSchemaSourceFormats(t *testing.T) {
	policy := atlascompatpolicy.StrictCE()
	for _, source := range []string{
		"schema.yaml",
		"schema.YML",
		"file://schema.yaml",
		"file:///tmp/schema.yml?mode=inspect",
	} {
		t.Run(source, func(t *testing.T) {
			c := qt.New(t)
			err := policy.ValidateLocalSchemaSource(source)

			c.Assert(err, qt.ErrorMatches,
				`Atlas Community Edition strict compatibility does not support YAML schema source ".+"`)
			c.Assert(atlascompatpolicy.Full().ValidateLocalSchemaSource(source), qt.IsNil)
		})
	}

	for _, source := range []string{
		"schema.hcl",
		"file://schema.sql",
		"env://schema.src",
		"sqlite://schema.yaml",
	} {
		t.Run("accepts "+source, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(policy.ValidateLocalSchemaSource(source), qt.IsNil)
		})
	}
}

func TestStrictCEValidatesProjectConfigSchemaSourceFormats(t *testing.T) {
	c := qt.New(t)
	err := atlascompatpolicy.StrictCE().ValidateProjectConfig(projectconfig.Config{
		SchemaSources: []string{"file://schema.yaml"},
	})

	c.Assert(err, qt.ErrorMatches,
		`Atlas Community Edition strict compatibility does not support YAML schema source "schema.yaml"`)
}

func TestStrictCERefusesMigrationContentExtensions(t *testing.T) {
	tests := []struct {
		name    string
		content string
		wantErr string
	}{
		{
			name:    "Atlas txtar",
			content: "-- atlas:txtar\n\n-- migration.sql --\nCREATE TABLE users (id integer);\n",
			wantErr: `Atlas Community Edition strict compatibility does not support Atlas txtar migration 1_users.sql`,
		},
		{
			name:    "Ptah check",
			content: `-- +ptah check name="ready" assert="SELECT 1"` + "\nSELECT 1;\n",
			wantErr: `Atlas Community Edition strict compatibility does not support Ptah pre-migration checks in 1_users.sql`,
		},
		{
			name:    "Ptah file directive",
			content: "-- +ptah no_transaction\nCREATE INDEX users_name_idx ON users (name);\n",
			wantErr: `Atlas Community Edition strict compatibility does not support Ptah migration directives in 1_users.sql`,
		},
		{
			name:    "bare Ptah directive marker",
			content: "-- +ptah\nSELECT 1;\n",
			wantErr: `Atlas Community Edition strict compatibility does not support Ptah migration directives in 1_users.sql`,
		},
		{
			name:    "unknown Ptah directive marker",
			content: "-- +ptah future_directive\nSELECT 1;\n",
			wantErr: `Atlas Community Edition strict compatibility does not support Ptah migration directives in 1_users.sql`,
		},
		{
			name:    "Atlas SQL template",
			content: "{{ if eq .Env \"production\" }}\nCREATE TABLE users (id integer);\n{{ end }}\n",
			wantErr: `Atlas Community Edition strict compatibility does not support SQL template migration 1_users.sql`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fsys := fstest.MapFS{"1_users.sql": {Data: []byte(test.content)}}

			err := atlascompatpolicy.StrictCE().ValidateMigrationSource(fsys)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(atlascompatpolicy.Full().ValidateMigrationSource(fsys), qt.IsNil)
		})
	}
}

func TestStrictCEMigrationContentValidationIgnoresDirectiveLookalikes(t *testing.T) {
	tests := []struct {
		name    string
		content string
	}{
		{
			name:    "same-line string literal",
			content: "SELECT '-- +ptah check name=\"fake\" assert=\"SELECT 1\"';\n",
		},
		{
			name: "multiline string literal",
			content: "INSERT INTO notes (body) VALUES ('runbook:\n" +
				"-- +ptah future_directive\n" +
				"done');\n",
		},
		{
			name:    "block comment",
			content: "/*\n-- +ptah future_directive\n*/\nSELECT 1;\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fsys := fstest.MapFS{"1_users.sql": {Data: []byte(test.content)}}

			err := atlascompatpolicy.StrictCE().ValidateMigrationSource(fsys)

			c.Assert(err, qt.IsNil)
		})
	}
}

func TestStrictCEMigrationContentValidationUsesTargetDialect(t *testing.T) {
	c := qt.New(t)
	mysqlString := "SELECT 'prefix \\'\n-- +ptah check name=\"fake\"\nsuffix';\n"
	fsys := fstest.MapFS{"1_users.sql": {Data: []byte(mysqlString)}}

	strictValidator := atlascompatpolicy.StrictCE().MigrationSourceValidator("mysql://localhost/app")
	c.Assert(strictValidator, qt.IsNotNil)
	c.Assert(atlascompatpolicy.Full().MigrationSourceValidator("mysql://localhost/app"), qt.IsNil)
	c.Assert(atlascompatpolicy.StrictCE().ValidateMigrationSourceForDialect(fsys, platform.MySQL), qt.IsNil)
	c.Assert(strictValidator(fsys), qt.IsNil)
	c.Assert(atlascompatpolicy.Full().ValidateMigrationSourceForURL(fsys, "mysql://localhost/app"), qt.IsNil)
	c.Assert(atlascompatpolicy.StrictCE().ValidateMigrationSource(fsys), qt.IsNil)

	actualDirective := fstest.MapFS{"1_users.sql": {Data: []byte(
		"SELECT 'prefix \\'suffix';\n-- +ptah no_transaction\nSELECT 1;\n",
	)}}
	c.Assert(atlascompatpolicy.StrictCE().ValidateMigrationSourceForDialect(actualDirective, platform.MySQL),
		qt.ErrorMatches,
		`Atlas Community Edition strict compatibility does not support Ptah migration directives in 1_users.sql`,
	)
	c.Assert(atlascompatpolicy.StrictCE().ValidateMigrationSourceForURL(actualDirective, "mysql://localhost/app"),
		qt.ErrorMatches,
		`Atlas Community Edition strict compatibility does not support Ptah migration directives in 1_users.sql`,
	)
}

// TestValidateRenderedVirtualTables pins which mode refuses a rendering that
// dropped a SQLite virtual table's module declaration.
//
// Strict compatibility owns the process output contract, so it refuses. Full
// mode matches the pinned community binary, which emits the same lossy empty
// table block and exits 0, and says so on the diagnostics stream instead.
func TestValidateRenderedVirtualTables(t *testing.T) {
	tests := []struct {
		name         string
		policy       atlascompatpolicy.Policy
		names        []string
		wantErr      bool
		wantContains string
	}{
		{
			name:         "strict refuses and names the table",
			policy:       atlascompatpolicy.StrictCE(),
			names:        []string{`"docs" (module fts5)`},
			wantErr:      true,
			wantContains: `"docs" (module fts5)`,
		},
		{
			name:         "strict names every dropped table",
			policy:       atlascompatpolicy.StrictCE(),
			names:        []string{`"docs" (module fts5)`, `"geo" (module rtree)`},
			wantErr:      true,
			wantContains: `"geo" (module rtree)`,
		},
		{
			name:         "strict points at the format that carries it",
			policy:       atlascompatpolicy.StrictCE(),
			names:        []string{`"docs" (module fts5)`},
			wantErr:      true,
			wantContains: "--format '{{ sql . }}'",
		},
		{
			name:    "strict with nothing dropped is not a refusal",
			policy:  atlascompatpolicy.StrictCE(),
			names:   nil,
			wantErr: false,
		},
		{
			name:    "full mode matches the pinned binary and does not refuse",
			policy:  atlascompatpolicy.Policy{},
			names:   []string{`"docs" (module fts5)`},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			err := tt.policy.ValidateRenderedVirtualTables(tt.names)

			c.Assert(err != nil, qt.Equals, tt.wantErr)
			c.Assert(policyErrorText(err), qt.Contains, tt.wantContains)
		})
	}
}

func policyErrorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
