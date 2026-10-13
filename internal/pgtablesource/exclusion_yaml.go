package pgtablesource

import (
	"fmt"

	"ptah.run/core/annotation"
	"ptah.run/core/schemaext"
	"ptah.run/core/yamlext"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/internal/yamlvalue"
)

// ExclusionYAML reads the exclusions section into named EXCLUDE objects.
// Each entry names its table and constraint; source order is preserved.
func ExclusionYAML() yamlext.Extension {
	return yamlext.Extension{Owner: pgtable.Owner, Kinds: []schemaext.Kind{pgtable.ExclusionKind},
		Sections: []yamlext.Section{{Key: "exclusions", Decode: decodeExclusionYAML}}, Coverage: exclusionCoverage}
}

type yamlExclusion struct {
	Name       yamlvalue.Scalar `yaml:"name"`
	Table      yamlvalue.Scalar `yaml:"table"`
	StructName yamlvalue.Scalar `yaml:"struct_name"`
	Using      yamlvalue.Scalar `yaml:"using"`
	Elements   yamlvalue.Scalar `yaml:"elements"`
	Condition  yamlvalue.Scalar `yaml:"condition"`
	Deferrable bool             `yaml:"deferrable"`
	Initially  yamlvalue.Scalar `yaml:"initially"`
	Comment    yamlvalue.Scalar `yaml:"comment"`
	Requires   []string         `yaml:"requires"`
	Dialects   yamlvalue.List   `yaml:"dialects"`
}

func decodeExclusionYAML(decode func(any) error, tables yamlext.Tables) ([]yamlext.Contribution, error) {
	var entries yamlvalue.OrderedMap[yamlExclusion]
	if err := decode(&entries); err != nil {
		return nil, err
	}
	declared := make(annotation.Tables, 0, len(tables))
	for _, table := range tables {
		declared = append(declared, annotation.Table{Schema: table.Schema, Name: table.Name, Struct: table.Struct})
	}
	var result []yamlext.Contribution
	var seen schemaext.Objects
	for _, entry := range entries {
		value := entry.Value
		name := entry.Name
		if value.Name != "" {
			name = string(value.Name)
		}
		schema, table, err := declared.Reference(string(value.StructName), string(value.Table), "an EXCLUDE constraint")
		if err != nil {
			return nil, fmt.Errorf("exclusions.%s: %w", entry.Name, err)
		}
		object, err := exclusionObject(schema, table, name, pgtable.DesiredExclusion{StructName: string(value.StructName), Exclusion: pgtable.Exclusion{
			UsingMethod: string(value.Using), Elements: string(value.Elements), WhereCondition: string(value.Condition),
			Deferrable: value.Deferrable, Initially: string(value.Initially), Comment: string(value.Comment), Dependencies: exclusionRequirements(value.Requires),
		}}, value.Dialects)
		if err != nil {
			return nil, fmt.Errorf("exclusions.%s: %w", entry.Name, err)
		}
		if seen, err = seen.Declare(object); err != nil {
			return nil, fmt.Errorf("exclusions.%s: %w", entry.Name, err)
		}
		result = append(result, yamlext.Contribution{Object: &object, Label: "EXCLUDE constraint " + object.Ref.String()})
	}
	return result, nil
}
