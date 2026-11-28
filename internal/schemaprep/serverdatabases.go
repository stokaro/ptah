package schemaprep

import (
	"slices"
	"strings"

	"ptah.run/core/schemamodel"
)

// ServerDatabases is the databases desired declares, in declaration order:
// each schema block, then each database a table or a sequence names that no
// block does, with no attribute of its own.
//
// The comparison plans the removal of a database the server holds and this
// list lacks, and the --include pairing keeps on both sides of a comparison a
// database this list names. One rule serves both: a pairing that read only the
// schema blocks would let a database declared through its tables lose its
// pairing, and the comparison would then plan its drop.
func ServerDatabases(desired *schemamodel.Database) []schemamodel.Schema {
	if desired == nil {
		return nil
	}
	databases := slices.Clone(desired.Schemas)
	named := func(name string) {
		if strings.TrimSpace(name) == "" {
			return
		}
		if slices.ContainsFunc(databases, func(schema schemamodel.Schema) bool { return schema.Name == name }) {
			return
		}
		databases = append(databases, schemamodel.Schema{Name: name})
	}
	for _, table := range desired.Tables {
		named(table.Schema)
	}
	for _, sequence := range desired.Sequences {
		named(sequence.Schema)
	}
	return databases
}
