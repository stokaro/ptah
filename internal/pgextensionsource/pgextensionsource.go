// Package pgextensionsource reads PostgreSQL extension declarations for the
// owner of package pgextension from every source: the //ptah:schema:extension
// directive, the YAML extensions key, the HCL extension block, and CREATE
// EXTENSION and COMMENT ON EXTENSION in SQL. Each frontend hands it the
// declarations; it returns the extensions they declare and claims that the
// source describes every extension.
package pgextensionsource

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"ptah.run/core/annotation"
	"ptah.run/core/coverage"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/yamlext"
	"ptah.run/feature/pgextension"
	"ptah.run/internal/yamlvalue"
)

// Directive is the Go annotation that declares an extension.
const Directive = "ptah:schema:extension"

// Key is the YAML document key the owner reads extensions from, one entry per
// extension keyed by its name.
const Key = "extensions"

// Family are the targets an extension declaration scoped to them declares:
// the PostgreSQL family, as [platform.IsPostgresFamily] names it.
func Family() []string {
	return []string{platform.Postgres, platform.CockroachDB, platform.YugabyteDB, platform.Spanner}
}

// Annotations is the extension owner's contribution to the Go annotation
// frontend. It reads the extension declarations that name no target, or only
// PostgreSQL-family targets, and claims that a Go annotation source describes
// every extension: one the source leaves out is absent.
func Annotations() annotation.Extension {
	return annotation.Extension{
		Owner: pgextension.Owner,
		Kinds: []schemaext.Kind{pgextension.ExtensionKind},
		TargetScopes: []annotation.TargetScope{{Directive: Directive, Targets: Family(), Unscoped: true,
			Label: "PostgreSQL-family targets"}},
		Decode: decodeDirective,
		Coverage: func([]coverage.Object) (schemaext.Coverage, error) {
			return pgextension.CompleteCoverage(schemaext.Desired)
		},
	}
}

// YAML is the extension owner's contribution to the YAML frontend: the
// extensions key, which declares extensions as the directive does, and the
// same claim.
func YAML() yamlext.Extension {
	return yamlext.Extension{
		Owner:    pgextension.Owner,
		Kinds:    []schemaext.Kind{pgextension.ExtensionKind},
		Sections: []yamlext.Section{{Key: Key, Decode: decodeYAML}},
		Coverage: func() (schemaext.Coverage, error) { return pgextension.CompleteCoverage(schemaext.Desired) },
	}
}

// decodeDirective reads one //ptah:schema:extension declaration.
func decodeDirective(declaration annotation.Declaration) ([]annotation.Contribution, error) {
	attributes := declaration.Attributes
	object, err := Declare(attributes["name"], pgextension.DesiredExtension{Schema: attributes["schema"], Version: attributes["version"],
		Comment: attributes["comment"], IfNotExists: attributes["if_not_exists"] == "true"})
	if err != nil {
		return nil, &annotation.DeclarationError{Declaration: declaration, Attribute: "name", Err: err}
	}
	object.Targets = declaration.Targets
	return []annotation.Contribution{{Object: &object, Source: declaration, Targets: declaration.Targets,
		Label: fmt.Sprintf("extension %q", attributes["name"])}}, nil
}

// yamlExtension is one entry of the extensions key. The entry's key names the
// extension unless name does.
type yamlExtension struct {
	Name        yamlvalue.Scalar `yaml:"name"`
	Schema      yamlvalue.Scalar `yaml:"schema"`
	IfNotExists bool             `yaml:"if_not_exists"`
	Version     yamlvalue.Scalar `yaml:"version"`
	Comment     yamlvalue.Scalar `yaml:"comment"`
}

// decodeYAML reads the extensions key, in the order of the entries' keys.
func decodeYAML(decode func(target any) error, _ yamlext.Tables) ([]yamlext.Contribution, error) {
	var entries map[string]yamlExtension
	if err := decode(&entries); err != nil {
		return nil, err
	}
	contributions := make([]yamlext.Contribution, 0, len(entries))
	for _, key := range slices.Sorted(maps.Keys(entries)) {
		entry := entries[key]
		name := string(entry.Name)
		if name == "" {
			name = key
		}
		object, err := Declare(name, pgextension.DesiredExtension{Schema: string(entry.Schema), Version: string(entry.Version),
			Comment: string(entry.Comment), IfNotExists: entry.IfNotExists})
		if err != nil {
			return nil, fmt.Errorf("%s.%s: %w", Key, key, err)
		}
		contributions = append(contributions, yamlext.Contribution{Object: &object, Label: fmt.Sprintf("extension %q", name)})
	}
	return contributions, nil
}

// Declare returns the declaration of the extension name as the owner's
// object. Every source reads an extension through it, so one declaration
// decodes to one object from any of them. A blank name is refused.
func Declare(name string, extension pgextension.DesiredExtension) (schemaext.Object, error) {
	if strings.TrimSpace(name) == "" {
		return schemaext.Object{}, fmt.Errorf("%w: an extension declaration requires a name", schemaext.ErrInvalidValue)
	}
	return pgextension.DesiredObject(pgextension.Ref(name), extension)
}
