package schema_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/internal/exitcode"
)

// graphqlModel is a schema with a server-generated key, a defaulted column, a
// server-rewritten column, and a credential-shaped column, so the operation
// profiles below can be checked against a write projection rather than against
// "every non-serial column".
const graphqlModel = `package models

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="email" type="VARCHAR(255)" not_null="true"
	Email string

	//ptah:schema:field name="password_hash" type="VARCHAR(255)" not_null="true"
	PasswordHash string

	//ptah:schema:field name="role" type="VARCHAR(32)" not_null="true" default="member"
	Role string
}
`

func writeGraphQLModel(c *qt.C) string {
	c.Helper()
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "model.go"), []byte(graphqlModel), 0o600), qt.IsNil)
	return dir
}

// TestSchemaExportGraphQLDefaultIsTypesOnly is the acceptance check for the
// default: an invocation that asks for nothing gets data types and no operation
// surface, because Ptah generates no resolvers or authorization to stand behind
// one.
func TestSchemaExportGraphQLDefaultIsTypesOnly(t *testing.T) {
	c := qt.New(t)
	dir := writeGraphQLModel(c)

	stdout, stderr, err := runSchemaExport("--to", "graphql", "--root-dir", dir)

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	c.Assert(stdout, qt.Contains, "type User {")

	absent := []string{"type Query", "input ", "Connection", "Edge", "PageInfo"}
	for _, marker := range absent {
		c.Assert(stdout, qt.Not(qt.Contains), marker,
			qt.Commentf("default export must not contain %q", marker))
	}
}

// TestSchemaExportGraphQLExplicitNoneMatchesDefault pins the spelling that
// writes the default down.
func TestSchemaExportGraphQLExplicitNoneMatchesDefault(t *testing.T) {
	c := qt.New(t)
	dir := writeGraphQLModel(c)

	base, _, err := runSchemaExport("--to", "graphql", "--root-dir", dir)
	c.Assert(err, qt.IsNil)
	explicit, _, err := runSchemaExport("--to", "graphql", "--root-dir", dir, "--graphql-operations", "none")
	c.Assert(err, qt.IsNil)

	c.Assert(explicit, qt.Equals, base)
}

func TestSchemaExportGraphQLOperationProfiles(t *testing.T) {
	c := qt.New(t)
	dir := writeGraphQLModel(c)

	tests := []struct {
		name    string
		profile string
		present []string
		absent  []string
	}{
		{
			name:    "list",
			profile: "list",
			present: []string{
				"type Query {", "users(first: Int, after: String): UserConnection",
				"type UserEdge {", "type UserConnection {", "type PageInfo {",
			},
			absent: []string{"input ", "user(id: ID!)"},
		},
		{
			name:    "by id",
			profile: "by-id",
			present: []string{"type Query {", "user(id: ID!): User"},
			absent:  []string{"input ", "Connection", "PageInfo"},
		},
		{
			name:    "create input",
			profile: "create-input",
			present: []string{"input UserCreateInput {", "email: String!", "role: String\n"},
			absent:  []string{"type Query", "UserUpdateInput", "Connection"},
		},
		{
			name:    "update input",
			profile: "update-input",
			present: []string{"input UserUpdateInput {", "email: String\n"},
			absent:  []string{"type Query", "UserCreateInput", "Connection"},
		},
		{
			name:    "both query shapes",
			profile: "list,by-id",
			present: []string{"users(first: Int, after: String): UserConnection", "user(id: ID!): User"},
			absent:  []string{"input "},
		},
		{
			name:    "both inputs",
			profile: "create-input,update-input",
			present: []string{"input UserCreateInput {", "input UserUpdateInput {"},
			absent:  []string{"type Query", "Connection"},
		},
		{
			name:    "every shape",
			profile: "list,by-id,create-input,update-input",
			present: []string{
				"type Query {", "users(first: Int, after: String): UserConnection",
				"user(id: ID!): User", "input UserCreateInput {", "input UserUpdateInput {",
			},
			absent: []string{"UserInput"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			stdout, stderr, err := runSchemaExport(
				"--to", "graphql", "--root-dir", dir, "--graphql-operations", test.profile)

			c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
			c.Assert(stdout, qt.Contains, "Ptah generates no resolvers")
			for _, marker := range test.present {
				c.Assert(stdout, qt.Contains, marker,
					qt.Commentf("profile %q must contain %q", test.profile, marker))
			}
			for _, marker := range test.absent {
				c.Assert(stdout, qt.Not(qt.Contains), marker,
					qt.Commentf("profile %q must not contain %q", test.profile, marker))
			}
		})
	}
}

// TestSchemaExportGraphQLInputsExcludeServerOwnedColumns pins the write
// projection through the CLI: the serial key never reaches an input, the
// defaulted column is optional on create, and the key is gone from the update
// shape.
func TestSchemaExportGraphQLInputsExcludeServerOwnedColumns(t *testing.T) {
	c := qt.New(t)
	dir := writeGraphQLModel(c)

	stdout, stderr, err := runSchemaExport(
		"--to", "graphql", "--root-dir", dir, "--graphql-operations", "create-input,update-input")

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	c.Assert(stdout, qt.Contains,
		"input UserCreateInput {\n  email: String!\n  password_hash: String!\n  role: String\n}")
	c.Assert(stdout, qt.Contains,
		"input UserUpdateInput {\n  email: String\n  password_hash: String\n  role: String\n}")
}

func TestSchemaExportGraphQLOperationsWritesToFile(t *testing.T) {
	c := qt.New(t)
	dir := writeGraphQLModel(c)
	outPath := filepath.Join(dir, "schema.graphql")

	stdout, stderr, err := runSchemaExport(
		"--to", "graphql", "--root-dir", dir, "--out", outPath, "--graphql-operations", "list")

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	c.Assert(stdout, qt.Contains, "Exported GraphQL schema to ")
	content, err := os.ReadFile(outPath)
	c.Assert(err, qt.IsNil)
	c.Assert(string(content), qt.Contains, "type Query {")
}

func TestSchemaExportGraphQLOperationsFailurePath(t *testing.T) {
	c := qt.New(t)
	dir := writeGraphQLModel(c)

	tests := []struct {
		name    string
		value   string
		wantErr string
	}{
		{
			name:    "unknown shape",
			value:   "mutations",
			wantErr: `--graphql-operations: unknown GraphQL operation "mutations": expected none, list, by-id, create-input, or update-input`,
		},
		{
			name:    "none combined with a shape",
			value:   "none,list",
			wantErr: `--graphql-operations: GraphQL operation "none" selects a types-only schema and cannot be combined with list`,
		},
		{
			name:    "one bad value among good ones",
			value:   "list,by-ID",
			wantErr: `--graphql-operations: unknown GraphQL operation "by-ID": expected none, list, by-id, create-input, or update-input`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			stdout, stderr, err := runSchemaExport(
				"--to", "graphql", "--root-dir", dir, "--graphql-operations", test.value)

			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(test.wantErr))
			c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
			c.Assert(stderr, qt.Contains, "error: "+test.wantErr+"\n")
			// A refused selection must emit no schema at all.
			c.Assert(stdout, qt.Equals, "")
		})
	}
}

// TestSchemaExportGraphQLOperationsRejectedOnOtherTargets keeps the selector
// from being accepted where it would silently do nothing.
func TestSchemaExportGraphQLOperationsRejectedOnOtherTargets(t *testing.T) {
	c := qt.New(t)
	dir := writeGraphQLModel(c)
	wantErr := "--graphql-operations is only supported with --to graphql"

	tests := []struct {
		name string
		args []string
	}{
		{name: "openapi", args: []string{"--to", "openapi-v3", "--root-dir", dir}},
		{
			name: "protobuf",
			args: []string{
				"--to", "protobuf", "--root-dir", dir,
				"--out", filepath.Join(dir, "schema.proto"), "--proto-package", "acme.inventory.v1",
			},
		},
		{name: "hcl", args: []string{"--to", "hcl", "--root-dir", dir, "--out", filepath.Join(dir, "schema.hcl")}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			args := append(append([]string{}, test.args...), "--graphql-operations", "list")

			stdout, stderr, err := runSchemaExport(args...)

			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(wantErr))
			c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
			c.Assert(stderr, qt.Contains, "error: "+wantErr+"\n")
			c.Assert(stdout, qt.Equals, "")
		})
	}
}

// TestSchemaExportGraphQLRefusalHappensBeforeWriting checks that a rejected
// selector stops the run before the protobuf target touches its committed
// compatibility state.
func TestSchemaExportGraphQLRefusalHappensBeforeWriting(t *testing.T) {
	c := qt.New(t)
	dir := writeGraphQLModel(c)
	outPath := filepath.Join(dir, "schema.proto")

	_, _, err := runSchemaExport(
		"--to", "protobuf", "--root-dir", dir, "--out", outPath,
		"--proto-package", "acme.inventory.v1", "--graphql-operations", "list")

	c.Assert(err, qt.IsNotNil)
	_, statErr := os.Stat(outPath)
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
}

func TestSchemaExportGraphQLHelpDocumentsTheDefault(t *testing.T) {
	c := qt.New(t)

	stdout, stderr, err := runSchemaExport("--help")

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	c.Assert(stdout, qt.Contains, "--graphql-operations")
	c.Assert(stdout, qt.Contains, "the default is a types-only schema")
	c.Assert(strings.Count(stdout, "--graphql-operations") >= 2, qt.IsTrue)
}
