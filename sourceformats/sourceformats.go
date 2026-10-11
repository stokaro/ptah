// Package sourceformats reads the output of an external schema program in
// each format ptah.run/core/schemasource accepts, with the bundled readers:
// YAML, SQL and HCL with the feature owners a caller selects, through the
// readers the native and compatibility binaries use. Those readers link the
// owners whose syntax they still decode themselves, so the neutral
// core/schemasource takes them from its caller rather than importing them.
package sourceformats

import (
	"context"

	"ptah.run/core/coverage"
	"ptah.run/core/hclext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemasource"
	"ptah.run/core/sqlext"
	"ptah.run/core/yamlext"
	"ptah.run/core/yamlschema"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/sqlschema"
)

// New returns the readers with owners selecting the feature owners a YAML
// document may declare, sql the owners whose syntax a SQL document may carry,
// hcl the owners whose blocks an HCL document may carry, and vocabulary the
// coverage kinds an SQL or HCL document's header may name. SQL and HCL are
// read by the bundled readers. Reading SQL refuses the zero sql set, and
// reading HCL the zero hcl set; pass sqlext.None or hclext.None to read the
// reader's own syntax only.
func New(owners yamlext.Set, sql sqlext.Set, hcl hclext.Set, vocabulary coverage.Vocabulary) schemasource.Formats {
	return readers{owners: owners, sql: sql, hcl: hcl, vocabulary: vocabulary}
}

type readers struct {
	owners     yamlext.Set
	sql        sqlext.Set
	hcl        hclext.Set
	vocabulary coverage.Vocabulary
}

// ReadYAML reads a YAML document with the selected owners.
func (r readers) ReadYAML(data []byte) (*schemamodel.Database, error) {
	return yamlschema.Parse(r.owners, data)
}

// ReadSQL reads SQL DDL with an optional dialect hint.
func (r readers) ReadSQL(data []byte, dialect string) (*schemamodel.Database, error) {
	// The program has already run when its output is read, and the owners
	// read their syntax in process, so the parse bounds the read.
	db, _, err := sqlschema.ReadWith(context.Background(), data, dialect, nil,
		sqlschema.Options{Owners: r.sql, Vocabulary: r.vocabulary})
	if err != nil {
		return nil, err
	}
	return &db, nil
}

// ReadHCL reads an Atlas HCL document, naming it filename in a refusal.
func (r readers) ReadHCL(data []byte, filename string) (*schemamodel.Database, error) {
	return atlashcl.ParseWithOptions(data, filename, atlashcl.Options{CoverageVocabulary: r.vocabulary, HCL: r.hcl})
}
