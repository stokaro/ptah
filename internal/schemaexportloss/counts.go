package schemaexportloss

import (
	"fmt"
	"sort"
)

// Descriptions returns positive counts as sorted "property (count)" entries.
func Descriptions(counts map[string]int) []string {
	result := make([]string, 0, len(counts))
	for name, count := range counts {
		if count > 0 {
			result = append(result, fmt.Sprintf("%s (%d)", name, count))
		}
	}
	sort.Strings(result)
	return result
}
