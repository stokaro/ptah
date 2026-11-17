// Package pgrolesource reads and writes PostgreSQL role settings in source
// formats: the settings and reset_settings attributes of a Go role directive
// and a YAML role entry, and ALTER ROLE ... SET and RESET in SQL. Every format
// reads a value with one grammar, the one pgrole.ParseValue gives, and the
// frontends keep the syntax they own.
//
// Atlas HCL has no attribute for a role's settings, so the HCL frontend reads
// none and refuses one as an unknown role attribute; a role read from HCL
// leaves its settings unmanaged.
package pgrolesource

import (
	"fmt"
	"strings"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/postgres/pgrole"
)

// The attributes a role declaration spells its settings with, in Go
// annotations and in YAML alike.
const (
	// SettingsAttribute holds the values, `name = value` separated by
	// semicolons, each value written in SQL: settings="statement_timeout =
	// '30s'; search_path = '$user', public".
	SettingsAttribute = "settings"
	// ResetAttribute names the settings to reset, separated by commas:
	// reset_settings="work_mem, app.tenant". A reset is how a declaration
	// takes a setting away; omitting a setting leaves it as the server holds
	// it.
	ResetAttribute = "reset_settings"
)

// Coverage states that a source describes the settings every role it declares
// asks for. A role without them manages none, so the claim never asks for a
// setting to be taken away.
func Coverage() (schemaext.Coverage, error) {
	return pgrole.SettingsCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)
}

// Decode reads the two attributes a declaration wrote into a role's facets.
// It refuses an attribute that lists nothing, a value or a name pgrole refuses,
// a setting named twice, DEFAULT and FROM CURRENT.
func Decode(attributes map[string]string) (schemaext.Facets, error) {
	var settings []pgrole.Setting
	if written, found := attributes[SettingsAttribute]; found {
		values, err := ParseSettings(written)
		if err != nil {
			return schemaext.Facets{}, err
		}
		settings = append(settings, values...)
	}
	if written, found := attributes[ResetAttribute]; found {
		resets, err := ParseResets(written)
		if err != nil {
			return schemaext.Facets{}, err
		}
		settings = append(settings, resets...)
	}
	return pgrole.WithSettings(schemaext.Facets{}, settings)
}

// ParseSettings reads `name = value` pairs separated by semicolons, the value
// written in SQL. A semicolon inside a quoted part belongs to the value.
func ParseSettings(written string) ([]pgrole.Setting, error) {
	parts := splitOutsideQuotes(written, ';')
	var settings []pgrole.Setting
	for _, part := range parts {
		if strings.TrimSpace(part) == "" {
			continue
		}
		setting, err := parseAssignment(part)
		if err != nil {
			return nil, err
		}
		settings = append(settings, setting)
	}
	if len(settings) == 0 {
		return nil, fmt.Errorf("%w: %s lists no setting", schemaext.ErrInvalidValue, SettingsAttribute)
	}
	return settings, nil
}

func parseAssignment(written string) (pgrole.Setting, error) {
	parts := splitOutsideQuotes(written, '=')
	if len(parts) < 2 {
		if fields := strings.Fields(strings.ToUpper(written)); len(fields) >= 3 && fields[len(fields)-2] == "FROM" && fields[len(fields)-1] == "CURRENT" {
			return pgrole.Setting{}, fmt.Errorf("%w: role setting %q: FROM CURRENT stores the value of the session that runs the statement, "+
				"which no declaration can compare; write the value", schemaext.ErrInvalidValue, strings.TrimSpace(written))
		}
		return pgrole.Setting{}, fmt.Errorf("%w: role setting %q is not name = value", schemaext.ErrInvalidValue, strings.TrimSpace(written))
	}
	name, err := pgrole.ParseName(parts[0])
	if err != nil {
		return pgrole.Setting{}, err
	}
	value, form, err := pgrole.ParseValue(strings.Join(parts[1:], "="))
	if err != nil {
		return pgrole.Setting{}, err
	}
	if form == pgrole.ValueDefault {
		return pgrole.Setting{}, fmt.Errorf("%w: role setting %q is DEFAULT, which resets it; name it in %s instead", schemaext.ErrInvalidValue, name, ResetAttribute)
	}
	return pgrole.Setting{Name: name, Value: value}, nil
}

// ParseResets reads setting names separated by commas. It refuses ALL: a
// declaration resets the settings it names and never the ones it does not.
func ParseResets(written string) ([]pgrole.Setting, error) {
	var settings []pgrole.Setting
	for _, part := range splitOutsideQuotes(written, ',') {
		if strings.TrimSpace(part) == "" {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(part), "ALL") {
			return nil, fmt.Errorf("%w: %s names ALL, which would reset settings no declaration names; name each setting", schemaext.ErrInvalidValue, ResetAttribute)
		}
		name, err := pgrole.ParseName(part)
		if err != nil {
			return nil, err
		}
		settings = append(settings, pgrole.Setting{Name: name, Reset: true})
	}
	if len(settings) == 0 {
		return nil, fmt.Errorf("%w: %s lists no setting", schemaext.ErrInvalidValue, ResetAttribute)
	}
	return settings, nil
}

// splitOutsideQuotes splits text at each separator outside single and double
// quotes. A doubled quote inside a quoted part stays in it.
func splitOutsideQuotes(text string, separator byte) []string {
	var parts []string
	var quote byte
	start := 0
	for i := 0; i < len(text); i++ {
		char := text[i]
		switch {
		case quote != 0:
			if char == quote {
				quote = 0
			}
		case char == '\'' || char == '"':
			quote = char
		case char == separator:
			parts = append(parts, text[start:i])
			start = i + 1
		}
	}
	return append(parts, text[start:])
}

// Encode writes the settings of facets as the two attributes, in name order:
// SettingsAttribute for the values and ResetAttribute for the resets, each
// left out when it holds nothing. It is what Decode reads back.
func Encode(facets schemaext.Facets) (settings, resets string, err error) {
	declared, found, err := schemaext.FacetAs[*pgrole.DesiredSettings](facets, pgrole.SettingsKind)
	if err != nil || !found {
		return "", "", err
	}
	if err := pgrole.ValidateDesiredSettings(declared); err != nil {
		return "", "", err
	}
	var values, names []string
	for _, setting := range declared.Settings {
		if setting.Reset {
			names = append(names, pgrole.QuoteName(setting.Name))
			continue
		}
		values = append(values, pgrole.QuoteName(setting.Name)+" = "+setting.Value)
	}
	return strings.Join(values, "; "), strings.Join(names, ", "), nil
}
