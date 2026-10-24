// Package sourceformats reads the output of an external schema program in
// each format ptah.run/core/schemasource accepts, with the bundled readers:
// YAML, SQL and HCL with the feature owners a caller selects, through the
// readers the native and compatibility binaries use. Those readers link the
// owners whose syntax they still decode themselves, so the neutral
// core/schemasource takes them from its caller rather than importing them.
package sourceformats

import (
	"context"
	"io"

	"ptah.run/core/coverage"
	"ptah.run/core/hclext"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemasource"
	"ptah.run/core/sqlext"
	"ptah.run/core/yamlext"
	"ptah.run/core/yamlschema"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/rundiag"
	"ptah.run/internal/sqlschema"
)

// Relations says which table a standalone feature object is attached to; an
// engine runtime is one. A SQL document's DROP TABLE removes the objects
// attached to the table, as the server drops them with it.
type Relations interface {
	schemaext.RelationRuntime
	RelationKinds(target string, representation schemaext.Representation) ([]schemaext.Kind, error)
}

// New returns the readers with owners selecting the feature owners a YAML
// document may declare, sql the owners whose syntax a SQL document may carry,
// hcl the owners whose blocks an HCL document may carry, vocabulary the
// coverage kinds an SQL or HCL document's header may name, and relations the
// tables a SQL document's standalone objects are attached to. SQL and HCL are
// read by the bundled readers. Reading SQL refuses the zero sql set, and
// reading HCL the zero hcl set; pass sqlext.None or hclext.None to read the
// reader's own syntax only. With nil relations, a DROP TABLE in a SQL
// document whose model holds a standalone feature object is refused.
func New(owners yamlext.Set, sql sqlext.Set, hcl hclext.Set, vocabulary coverage.Vocabulary, relations Relations) schemasource.Formats {
	return readers{owners: owners, sql: sql, hcl: hcl, vocabulary: vocabulary, relations: relations}
}

// NewWithNotices returns the readers [New] returns, which also write a warning
// line to notices for each name a SQL document writes longer than the
// identifier limit of a server that cuts it, naming source as what was read.
// The document's objects are recorded under the name the server keeps either
// way. A nil notices writes nothing, as [New] does.
func NewWithNotices(owners yamlext.Set, sql sqlext.Set, hcl hclext.Set, vocabulary coverage.Vocabulary, relations Relations, notices io.Writer, source string) schemasource.Formats {
	r := readers{owners: owners, sql: sql, hcl: hcl, vocabulary: vocabulary, relations: relations}
	r.notices, r.source = notices, source
	return r
}

type readers struct {
	owners     yamlext.Set
	sql        sqlext.Set
	hcl        hclext.Set
	vocabulary coverage.Vocabulary
	relations  Relations
	notices    io.Writer
	source     string
}

// ReadYAML reads a YAML document with the selected owners.
func (r readers) ReadYAML(ctx context.Context, data []byte) (*schemamodel.Database, error) {
	return yamlschema.Parse(ctx, r.owners, data)
}

// ReadSQL reads SQL DDL with an optional dialect hint.
func (r readers) ReadSQL(ctx context.Context, data []byte, dialect string) (*schemamodel.Database, error) {
	// The command deadline includes interpretation by selected owners.
	opts := sqlschema.Options{Owners: r.sql, Vocabulary: r.vocabulary, Relations: r.relations, Source: r.source}
	if r.notices != nil {
		opts.StoredNames = func(name sqlschema.StoredName) {
			_ = rundiag.Warn(ctx, r.notices, rundiag.TruncatedNames, sqlschema.StoredNameRecord(r.source, name))
		}
		opts.Redeclared = func(redeclared sqlschema.Redeclaration) {
			_ = rundiag.Warn(ctx, r.notices, rundiag.RedeclaredConstraints, sqlschema.RedeclarationRecord(r.source, redeclared))
		}
	}
	db, _, err := sqlschema.ReadWith(ctx, data, dialect, nil, opts)
	if err != nil {
		return nil, err
	}
	return &db, nil
}

// ReadHCL reads an Atlas HCL document, naming it filename in a refusal.
func (r readers) ReadHCL(ctx context.Context, data []byte, filename, dialect string) (*schemamodel.Database, error) {
	return atlashcl.ParseWithOptions(ctx, data, filename, atlashcl.Options{CoverageVocabulary: r.vocabulary, HCL: r.hcl, Dialect: dialect})
}
