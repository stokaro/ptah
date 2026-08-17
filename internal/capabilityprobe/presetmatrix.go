package capabilityprobe

import (
	"fmt"
	"io"
	"slices"
	"strings"

	"go.5x5.cz/ptah/core/platform/capability"
)

// WritePresetMarkdown prints the capability-by-preset table docs/capabilities.md
// carries.
//
// It is generated because the hand-written version went stale in both
// directions at once: it was missing five keys and four presets by the time
// stokaro/ptah#916 looked at it, and the SQL Server column still declared two
// IF EXISTS guards absent that every supported line accepts. A table with one
// cell per (key, preset) has a few hundred of them, which is more than a
// reviewer can check by reading.
//
// Rows are sorted by key name rather than by registration order: the registry
// is a map, so registration order does not exist, and a stable order is what
// makes the generated block diffable.
func WritePresetMarkdown(w io.Writer) {
	presets := capability.NamedPresets()
	keys := capability.All()
	slices.Sort(keys)

	names := make([]string, 0, len(presets))
	for _, preset := range presets {
		names = append(names, preset.Name)
	}

	fmt.Fprintf(w, "| Capability | %s |\n", strings.Join(names, " | "))
	fmt.Fprintf(w, "|---|%s\n", strings.Repeat("---|", len(names)))
	for _, key := range keys {
		cells := make([]string, 0, len(presets))
		for _, preset := range presets {
			cells = append(cells, presetMark(preset.Capabilities, key))
		}
		fmt.Fprintf(w, "| `%s` | %s |\n", key, strings.Join(cells, " | "))
	}
}

// WriteCapabilityKeyMarkdown prints the key-to-meaning table docs/capabilities.md
// carries, from the registry's own doc strings.
//
// It is generated for the reason the preset table is: it had fallen five keys
// behind by the time stokaro/ptah#916 looked at it, and a registry entry with no
// documented meaning is exactly the kind of absence nobody notices.
func WriteCapabilityKeyMarkdown(w io.Writer) {
	keys := capability.All()
	slices.Sort(keys)

	fmt.Fprintf(w, "| Capability | Meaning |\n")
	fmt.Fprintf(w, "|---|---|\n")
	for _, key := range keys {
		fmt.Fprintf(w, "| `%s` | %s |\n", key, capability.Doc(key))
	}
}

// presetMark is what one cell prints. A preset that does not carry the key at
// all prints the same mark as one carrying it false, which is correct: Has
// answers false for both, and every reader of a capability set goes through
// Has.
func presetMark(caps capability.Capabilities, key capability.Capability) string {
	marks := map[bool]string{true: "✅", false: "❌"}
	return marks[caps.Has(key)]
}
