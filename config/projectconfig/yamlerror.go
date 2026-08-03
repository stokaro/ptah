package projectconfig

import (
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"go.yaml.in/yaml/v3"
)

// ptahConfigFlagName is the CLI flag that selects an explicit ptah.yaml path.
// It is spelled here rather than imported because cmd/internal/dbcli, which
// registers it, depends on this package. TestPtahConfigFlagNameMatchesCLI in
// cmd/internal/dbcli holds the two spellings together.
const ptahConfigFlagName = "config"

// The go-yaml strict decoder reports rejected input against the Go type it was
// decoding into: `field src not found in type projectconfig.yamlDocument`. That
// names a symbol the user cannot act on and cannot even find, so every decoder
// error is translated here into the ptah.yaml vocabulary before it reaches
// stderr. The translation is derived from the struct tags by reflection rather
// than from a hand-written table, so adding a ptah.yaml key updates the
// "supported keys are ..." list without a second edit.

var (
	// yamlFieldNotFoundPattern matches go-yaml's strict-mode unknown-key error.
	yamlFieldNotFoundPattern = regexp.MustCompile(`^line (\d+): field (\S+) not found in type (\S+)$`)
	// yamlCannotUnmarshalPattern matches go-yaml's type-mismatch error. The
	// middle group is the optional excerpt of the offending scalar.
	yamlCannotUnmarshalPattern = regexp.MustCompile(`^line (\d+): cannot unmarshal (\S+)(?: .*?)? into (\S+)$`)
	// yamlGoTypePattern is the fail-safe: any residual Go type name from this
	// package is replaced even when no specific rule above matched, so an
	// unanticipated decoder message cannot leak one.
	yamlGoTypePattern = regexp.MustCompile(`projectconfig\.[A-Za-z0-9_]+`)
)

// ptahYAMLSection describes where one ptah.yaml struct type sits in the
// document and which keys it accepts.
type ptahYAMLSection struct {
	// path is the dotted ptah.yaml path at which the type appears, empty for
	// the document root.
	path string
	// keys are the accepted YAML keys in declaration order.
	keys []string
}

// ptahYAMLSections maps a Go type name to its ptah.yaml position and keys.
var ptahYAMLSections = buildPtahYAMLSections()

// ptahDocumentTypeName is the Go type the whole document decodes into. A
// type-mismatch reported against it means the file is not a YAML mapping at
// all, which is what pointing --config at an atlas.hcl looks like.
var ptahDocumentTypeName = reflect.TypeFor[yamlDocument]().String()

func buildPtahYAMLSections() map[string]ptahYAMLSection {
	sections := make(map[string]ptahYAMLSection)
	visitPtahYAMLType(reflect.TypeFor[yamlDocument](), "", sections)
	return sections
}

func visitPtahYAMLType(typ reflect.Type, path string, sections map[string]ptahYAMLSection) {
	structType := ptahYAMLStructType(typ)
	if structType == nil {
		return
	}
	name := structType.String()
	if _, seen := sections[name]; seen {
		return
	}
	sections[name] = ptahYAMLSection{path: path, keys: ptahYAMLKeys(structType)}
	for field := range structType.Fields() {
		key, opts := splitYAMLTag(field.Tag.Get("yaml"))
		if strings.Contains(opts, "inline") {
			// An inline struct contributes its keys at the parent's level, so
			// it shares the parent's path.
			visitPtahYAMLType(field.Type, path, sections)
			continue
		}
		if key == "" || key == "-" {
			continue
		}
		visitPtahYAMLType(field.Type, joinYAMLPath(path, key), sections)
	}
}

// ptahYAMLKeys lists the YAML keys a struct accepts, expanding inline structs
// so the reported list matches what the strict decoder actually allows.
func ptahYAMLKeys(structType reflect.Type) []string {
	var keys []string
	for field := range structType.Fields() {
		key, opts := splitYAMLTag(field.Tag.Get("yaml"))
		if strings.Contains(opts, "inline") {
			if inlined := ptahYAMLStructType(field.Type); inlined != nil {
				keys = append(keys, ptahYAMLKeys(inlined)...)
			}
			continue
		}
		if key == "" || key == "-" {
			continue
		}
		keys = append(keys, key)
	}
	return keys
}

// ptahYAMLStructType unwraps pointers, slices, and maps down to a struct type,
// returning nil for anything that is not one.
func ptahYAMLStructType(typ reflect.Type) reflect.Type {
	for typ.Kind() == reflect.Pointer || typ.Kind() == reflect.Slice || typ.Kind() == reflect.Map {
		typ = typ.Elem()
	}
	if typ.Kind() != reflect.Struct {
		return nil
	}
	return typ
}

func splitYAMLTag(tag string) (key, opts string) {
	key, opts, _ = strings.Cut(tag, ",")
	return key, opts
}

func joinYAMLPath(path, key string) string {
	if path == "" {
		return key
	}
	return path + "." + key
}

// ptahYAMLSectionLabel names a section the way the user spells it in
// ptah.yaml.
func ptahYAMLSectionLabel(typeName string) string {
	section, ok := ptahYAMLSections[typeName]
	if !ok || section.path == "" {
		return "the " + PtahFileName + " document"
	}
	return "the " + PtahFileName + " " + section.path + " block"
}

// ptahConfigSource says how the config file was selected, which decides
// whether a diagnostic can point at the --config flag.
type ptahConfigSource int

const (
	// discoveredPtahConfig is the conventional ./ptah.yaml path.
	discoveredPtahConfig ptahConfigSource = iota
	// explicitPtahConfig is a path the user passed with --config.
	explicitPtahConfig
)

// translatePtahYAMLError rewrites a go-yaml decoder error into ptah.yaml terms.
// It never returns a message containing a Go type name from this package.
func translatePtahYAMLError(err error, filename string, source ptahConfigSource) error {
	lines := ptahYAMLErrorLines(err)

	// A type mismatch against the document type means the file is not a YAML
	// mapping. When the path came from --config, the actionable fact is what
	// --config accepts, not which YAML node kind was found.
	if found, line, ok := ptahYAMLRootMismatch(lines); ok {
		if source == explicitPtahConfig {
			return fmt.Errorf(
				"--%s takes a %s file, and %s is not a YAML mapping (line %s: found %s); an Atlas project config is discovered as ./%s and selected with --env",
				ptahConfigFlagName,
				PtahFileName,
				filename,
				line,
				found,
				AtlasFileName,
			)
		}
		return fmt.Errorf(
			"failed to parse ptah config %s: line %s: expected a YAML mapping of %s keys, found %s",
			filename,
			line,
			PtahFileName,
			found,
		)
	}

	translated := make([]string, 0, len(lines))
	for _, line := range lines {
		translated = append(translated, translatePtahYAMLLine(line))
	}
	if len(translated) == 1 {
		return fmt.Errorf("failed to parse ptah config %s: %s", filename, translated[0])
	}
	return fmt.Errorf(
		"failed to parse ptah config %s:\n  %s",
		filename,
		strings.Join(translated, "\n  "),
	)
}

// ptahYAMLErrorLines splits a decoder error into its individual complaints.
func ptahYAMLErrorLines(err error) []string {
	var typeErr *yaml.TypeError
	if errors.As(err, &typeErr) && len(typeErr.Errors) > 0 {
		return typeErr.Errors
	}
	return []string{scrubPtahYAMLGoTypes(err.Error())}
}

// ptahYAMLRootMismatch reports the node kind and line of a type mismatch
// against the document type, which is what a non-mapping file produces.
func ptahYAMLRootMismatch(lines []string) (found, line string, ok bool) {
	for _, raw := range lines {
		match := yamlCannotUnmarshalPattern.FindStringSubmatch(raw)
		if match != nil && match[3] == ptahDocumentTypeName {
			return match[2], match[1], true
		}
	}
	return "", "", false
}

func translatePtahYAMLLine(raw string) string {
	if match := yamlFieldNotFoundPattern.FindStringSubmatch(raw); match != nil {
		return ptahYAMLUnknownKey(match[1], match[2], match[3])
	}
	if match := yamlCannotUnmarshalPattern.FindStringSubmatch(raw); match != nil {
		if _, known := ptahYAMLSections[match[3]]; known {
			return fmt.Sprintf(
				"line %s: cannot read %s as %s",
				match[1],
				match[2],
				ptahYAMLSectionLabel(match[3]),
			)
		}
	}
	return scrubPtahYAMLGoTypes(raw)
}

func ptahYAMLUnknownKey(line, key, typeName string) string {
	section, known := ptahYAMLSections[typeName]
	if !known {
		return fmt.Sprintf("line %s: unknown %s key %q", line, PtahFileName, key)
	}
	where := ""
	if section.path != "" {
		where = " under " + section.path
	}
	return fmt.Sprintf(
		"line %s: unknown %s key %q%s; supported keys are %s",
		line,
		PtahFileName,
		key,
		where,
		strings.Join(section.keys, ", "),
	)
}

// scrubPtahYAMLGoTypes is the last line of defence for a decoder message no
// rule above recognized: it replaces any Go type name from this package with
// the ptah.yaml section it stands for.
func scrubPtahYAMLGoTypes(raw string) string {
	return yamlGoTypePattern.ReplaceAllStringFunc(raw, ptahYAMLSectionLabel)
}
