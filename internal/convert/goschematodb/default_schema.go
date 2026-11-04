package goschematodb

import (
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
)

// publicRole is the grantee that stands for every role.
const publicRole = "PUBLIC"

// WithDefaultSchemaBaseline returns current, the catalog [ToDBSchema] made of
// db, with what the server of dialect gives the default schema of every
// database and db leaves to it: USAGE for PUBLIC, as one [catalog.Grant]
// marked Implicit, and the schema's comment. current is not changed, and a
// dialect whose descriptor states neither returns it as it is.
//
// A comparison whose current side is a document reads that document as the
// database a server builds from it, and the database holds its default schema
// as the server created it. PostgreSQL creates schema public in every database
// with USAGE for PUBLIC and the comment `standard public schema`, and a read
// reports both. Left out, a diff from a schema file to a PostgreSQL 18
// database plans `GRANT USAGE ON SCHEMA "public" TO PUBLIC` for a privilege
// both hold, and `COMMENT ON SCHEMA "public"` for a file that declares the
// schema without a comment, while the reverse diff plans neither.
//
// [ToDBSchema] does not add them. Its result also says what a document
// declares, and a dev database's starting point is subtracted from a read by
// what is declared. Counted as declared, the default schema's privilege would
// stay in the inspected output of every document.
//
// The privilege is left out when db grants or revokes USAGE or ALL on the
// schema to PUBLIC itself: a grant is described by [ToDBSchema], and a revoke
// says the privilege is absent. The comment is added only to a schema db
// declares without one. A document that clears the comment holds no comment
// the model can tell from one it left out, so it compares as the server's.
//
// PostgreSQL 14 and earlier give PUBLIC CREATE on public as well. A dialect
// names no release, so against such a server a diff from a file still plans
// that GRANT.
func WithDefaultSchemaBaseline(current *catalog.Database, db *schemamodel.Database, dialect string) *catalog.Database {
	described, _ := platform.Lookup(dialect)
	schema, conversion := described.DefaultSchema, described.Conversion
	if current == nil || schema == "" {
		return current
	}
	out := *current
	if conversion.DefaultSchemaComment != "" {
		out.Schemas = slices.Clone(current.Schemas)
		for position := range out.Schemas {
			if out.Schemas[position].Name == schema && out.Schemas[position].Comment == "" {
				out.Schemas[position].Comment = conversion.DefaultSchemaComment
			}
		}
	}
	if conversion.DefaultSchemaUsage && !publicUsageDeclared(db, schema) {
		out.Grants = append(slices.Clone(current.Grants), catalog.Grant{
			Role:       publicRole,
			Privilege:  "USAGE",
			ObjectType: "SCHEMA",
			ObjectName: schema,
			Implicit:   true,
		})
	}
	return &out
}

// publicUsageDeclared reports whether db grants or revokes PUBLIC's USAGE on
// schema itself, either of which replaces the privilege the server gives it.
func publicUsageDeclared(db *schemamodel.Database, schema string) bool {
	if db == nil {
		return false
	}
	matches := func(grant schemamodel.Grant) bool {
		grant.Canonicalize()
		return strings.EqualFold(grant.Role, publicRole) && grant.OnSchema == schema &&
			(slices.Contains(grant.Privileges, "USAGE") || slices.Contains(grant.Privileges, "ALL"))
	}
	return slices.ContainsFunc(db.RevokedGrants, matches) || slices.ContainsFunc(db.Grants, matches)
}
