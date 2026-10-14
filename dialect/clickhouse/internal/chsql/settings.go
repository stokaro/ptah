package chsql

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/internal/lexer"
)

// Setting is one entry of a table's SETTINGS clause: the setting's name and
// its value as written.
type Setting struct {
	Name  string
	Value string
}

// implicitSetting is the one setting a MergeTree table carries without being
// asked: measured on 24.10, 25.8 and 26.9, `CREATE TABLE ... ENGINE = MergeTree
// ORDER BY a SETTINGS merge_with_ttl_timeout = 3600` reads back from
// system.tables.engine_full as `SETTINGS merge_with_ttl_timeout = 3600,
// index_granularity = 8192`. A declaration that leaves it out does not manage
// it, so the current state naming it alone is not a difference. A declaration
// that names it where the current state does not asks for a value the table
// was not created with, which is a difference.
const implicitSetting = "index_granularity"

// fixedSettings are the MergeTree settings the server fixes when a table is
// created. On 24.10, 25.8 and 26.9, MODIFY SETTING and RESET SETTING of
// index_granularity, index_granularity_bytes and enable_mixed_granularity_parts
// answer `Setting '...' is readonly for storage 'MergeTree'`. The rest are what
// system.merge_tree_settings marks readonly on 26.9 and refuses the same way;
// 25.8 refuses the add_minmax_index_for_* and table_disk ones it knows, and
// 24.10 knows none of them.
var fixedSettings = []string{
	"add_minmax_index_for_block_number_column", "add_minmax_index_for_block_offset_column",
	"add_minmax_index_for_numeric_columns", "add_minmax_index_for_string_columns",
	"add_minmax_index_for_temporal_columns", "allow_tuple_element_aggregation",
	"enable_mixed_granularity_parts", "index_granularity", "index_granularity_bytes",
	"share_nested_offsets", "table_disk",
}

// parseSettings reads a SETTINGS clause body as `name = value` entries in the
// order written. An entry that is not a name, an equals sign and a value, and a
// name written twice, are refused: the table's effective value of a repeated
// name is not something a plan can know.
func parseSettings(text string) ([]Setting, error) {
	var result []Setting
	for _, item := range splitList(text, significant(text)) {
		tokens := significant(item.text)
		if len(tokens) < 3 || tokens[0].Type != lexer.TokenIdentifier || !tokens[1].MatchOperatorValue("=") {
			return nil, fmt.Errorf("%w: ClickHouse SETTINGS entry %q is not `name = value`", schemaext.ErrInvalidValue, item.text)
		}
		name := tokens[0].Value
		if slices.ContainsFunc(result, func(setting Setting) bool { return setting.Name == name }) {
			return nil, fmt.Errorf("%w: ClickHouse SETTINGS names %s twice", schemaext.ErrInvalidValue, name)
		}
		result = append(result, Setting{Name: name, Value: item.text[tokens[2].Start:]})
	}
	return result, nil
}

// SettingsChange returns what changes the current SETTINGS clause into the
// declared one: the settings to set, in the order the declaration writes them,
// and the names to reset, in the order the current clause writes them. A
// setting is compared by its value's tokens, so spacing does not count and
// quoting does. index_granularity that only the current clause names is no
// change. A change to a setting the server fixes
// at creation is refused by name with ptaherr.ErrUnsupportedFeature, and a
// clause that cannot be read wraps schemaext.ErrInvalidValue.
func SettingsChange(before, after string) (set []Setting, reset []string, err error) {
	old, err := parseSettings(before)
	if err != nil {
		return nil, nil, err
	}
	current, err := parseSettings(after)
	if err != nil {
		return nil, nil, err
	}
	find := func(settings []Setting, name string) (Setting, bool) {
		index := slices.IndexFunc(settings, func(setting Setting) bool { return setting.Name == name })
		if index < 0 {
			return Setting{}, false
		}
		return settings[index], true
	}
	for _, setting := range current {
		previous, found := find(old, setting.Name)
		switch {
		case found && SameExpression(previous.Value, setting.Value):
			continue
		case slices.Contains(fixedSettings, setting.Name):
			return nil, nil, fixedSettingError(setting.Name)
		}
		set = append(set, setting)
	}
	for _, setting := range old {
		if _, found := find(current, setting.Name); found || setting.Name == implicitSetting {
			continue
		}
		if slices.Contains(fixedSettings, setting.Name) {
			return nil, nil, fixedSettingError(setting.Name)
		}
		reset = append(reset, setting.Name)
	}
	return set, reset, nil
}

func fixedSettingError(name string) error {
	return fmt.Errorf("%w: ClickHouse fixes table setting %s when the table is created; create the table again and copy its rows", ptaherr.ErrUnsupportedFeature, name)
}

// sameSettings compares the current SETTINGS clause with the declared one as
// sets of `name = value` entries, so their order does not count; MODIFY SETTING
// appends a setting the table did not have. Identical text is always the same,
// and a clause that cannot be read differs from every other text.
func sameSettings(current, declared string) bool {
	if SameExpression(current, declared) {
		return true
	}
	set, reset, err := SettingsChange(current, declared)
	if err != nil {
		// An unreadable clause differs, and so does a fixed setting.
		return false
	}
	return len(set) == 0 && len(reset) == 0
}

// Predicted returns the storage a table has after a change to declared: the
// declared clauses, and index_granularity from current when the declared
// settings leave it out, since the server keeps the value it records at
// creation. A prediction is planning input, never an observation.
func Predicted(current, declared *chschema.ObservedTable) *chschema.ObservedTable {
	result := *declared
	old, err := parseSettings(current.Settings)
	if err != nil {
		return &result
	}
	ours, err := parseSettings(declared.Settings)
	if err != nil || slices.ContainsFunc(ours, func(setting Setting) bool { return setting.Name == implicitSetting }) {
		return &result
	}
	for _, setting := range old {
		if setting.Name != implicitSetting {
			continue
		}
		entry := setting.Name + " = " + setting.Value
		if strings.TrimSpace(result.Settings) == "" {
			result.Settings = entry
		} else {
			result.Settings += ", " + entry
		}
	}
	return &result
}
