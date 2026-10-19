package atlashcl

import (
	"maps"
	"slices"

	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/descriptortable"
)

// propertyBlock is a block whose attributes an Atlas HCL schema reads from
// one engine's driver alone.
type propertyBlock int

const (
	tableProperties propertyBlock = iota
	columnProperties
	indexProperties
)

// propertyTargets maps each attribute of a block that the pinned community
// binary reads from one engine's driver alone onto the dialects that read it
// (descriptor.Atlas.HCL), in the order of the registry.
var propertyTargets = descriptortable.NewIndex(func(dialects []descriptor.Dialect) map[propertyBlock]map[string][]string {
	targets := map[propertyBlock]map[string][]string{tableProperties: {}, columnProperties: {}, indexProperties: {}}
	for _, described := range dialects {
		for block, names := range map[propertyBlock][]string{
			tableProperties:  described.Atlas.HCL.TableProperties,
			columnProperties: described.Atlas.HCL.ColumnProperties,
			indexProperties:  described.Atlas.HCL.IndexProperties,
		} {
			for _, name := range names {
				targets[block][name] = append(targets[block][name], described.Name)
			}
		}
	}
	return targets
})

// atlasProperties states options of a block, such as a table's
// auto_increment and charset, which only the MySQL family has, or its strict
// and without_rowid, which only SQLite has, as the platform properties of
// every target whose descriptor reads the attribute, where that target's
// owner reads it. An empty option states nothing, and a target group that
// states one already keeps its own. overrides is not changed.
func atlasProperties(overrides map[string]map[string]string, block propertyBlock, options map[string]string) map[string]map[string]string {
	if !slices.ContainsFunc(slices.Collect(maps.Values(options)), func(value string) bool { return value != "" }) {
		return overrides
	}
	targets := propertyTargets.Get()[block]
	result := make(map[string]map[string]string, len(overrides)+len(options))
	for target, group := range overrides {
		result[target] = maps.Clone(group)
	}
	for key, value := range options {
		if value == "" {
			continue
		}
		for _, target := range targets[key] {
			if result[target] == nil {
				result[target] = make(map[string]string)
			}
			if _, stated := result[target][key]; !stated {
				result[target][key] = value
			}
		}
	}
	return result
}
