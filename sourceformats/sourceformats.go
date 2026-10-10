// Package sourceformats reads the output of an external schema program in
// each format ptah.run/core/schemasource accepts, with the bundled readers:
// YAML with the feature owners a caller selects, and SQL and HCL with the
// readers the native and compatibility binaries use. Those readers link the
// owners whose syntax they still decode themselves, so the neutral
// core/schemasource takes them from its caller rather than importing them.
package sourceformats

import (
	"ptah.run/core/coverage"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemasource"
	"ptah.run/core/yamlext"
	"ptah.run/core/yamlschema"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/sqlschema"
)

// New returns the readers with owners selecting the feature owners a YAML
// document may declare, and vocabulary the coverage kinds an SQL or HCL
// document's header may name. SQL and HCL are read by the bundled readers.
func New(owners yamlext.Set, vocabulary coverage.Vocabulary) schemasource.Formats {
	return readers{owners: owners, vocabulary: vocabulary}
}

type readers struct {
	owners     yamlext.Set
	vocabulary coverage.Vocabulary
}

// ReadYAML reads a YAML document with the selected owners.
func (r readers) ReadYAML(data []byte) (*schemamodel.Database, error) {
	return yamlschema.Parse(r.owners, data)
}

// ReadSQL reads SQL DDL with an optional dialect hint.
func (r readers) ReadSQL(data []byte, dialect string) (*schemamodel.Database, error) {
	db, _, err := sqlschema.ReadOntoWithVocabulary(data, dialect, nil, r.vocabulary)
	if err != nil {
		return nil, err
	}
	return &db, nil
}

// ReadHCL reads an Atlas HCL document, naming it filename in a refusal.
func (r readers) ReadHCL(data []byte, filename string) (*schemamodel.Database, error) {
	return atlashcl.ParseWithOptions(data, filename, atlashcl.Options{CoverageVocabulary: r.vocabulary})
}
