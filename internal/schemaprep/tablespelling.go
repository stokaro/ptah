package schemaprep

import (
	"fmt"
	"sort"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/tableref"
)

// ValidateTableSpellings refuses a desired schema that declares one table twice
// under two spellings: without a schema, and qualified with defaultSchema.
//
// A table declared without a schema is created in the target's default schema,
// so `accounts` and `public.accounts` name one PostgreSQL table. The model keys
// a table by the name as written, which keeps the two apart: a composite of Go
// annotations and an HCL file, one spelling each, neither deduplicated them nor
// reported them as conflicting, and a render created the table twice
// (stokaro/ptah#3721).
//
// They are refused rather than merged. Which schema a bare name lands in is the
// target's answer, and a caller holding a static default can be wrong about a
// server whose search path says otherwise; merging on a wrong answer would
// quietly move a table, and refusing on one only asks the author to spell the
// table one way. An empty defaultSchema, for a target with none, checks
// nothing.
func ValidateTableSpellings(database *schemamodel.Database, defaultSchema string) error {
	if database == nil || defaultSchema == "" {
		return nil
	}
	bare := make(map[string]bool)
	qualified := make(map[string]bool)
	for _, table := range database.Tables {
		ref, ok := tableref.Parse(table.QualifiedName())
		if !ok {
			continue
		}
		switch {
		case !ref.Qualified:
			bare[ref.Name] = true
		case ref.Schema == defaultSchema:
			qualified[ref.Name] = true
		}
	}
	var twice []string
	for name := range bare {
		if qualified[name] {
			twice = append(twice, name)
		}
	}
	if len(twice) == 0 {
		return nil
	}
	sort.Strings(twice)
	name := twice[0]
	return fmt.Errorf(
		"table %q is declared twice, once without a schema and once as %q; a table without a schema "+
			"is created in the default schema %q, so both name one table -- declare it once, or spell it "+
			"the same way in every source",
		name, schemamodel.QualifyTableName(defaultSchema, name), defaultSchema,
	)
}
