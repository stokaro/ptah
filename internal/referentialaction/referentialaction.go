// Package referentialaction holds the one spelling of a foreign key's ON
// DELETE and ON UPDATE action. The built-in renderer writes it, the schema
// comparison compares it with the action a catalog reports, and the temporal
// key check reads it to refuse an action a PERIOD foreign key cannot take.
package referentialaction

import "strings"

// Spelling returns action in the SQL spelling: upper case, one space between
// words, and an underscore read as that space, the way an HCL document writes
// NO_ACTION. An empty or blank action is "".
//
// The renderer, the comparison and the temporal key check must read one
// spelling, so this cannot become two. The renderer writes the action it
// reads, and the comparison compares the action it reads with the catalog's;
// spelled two ways, one declaration renders an action the comparison then
// reads as another, and every plan replaces the foreign key.
func Spelling(action string) string {
	return strings.ToUpper(strings.Join(strings.Fields(strings.ReplaceAll(action, "_", " ")), " "))
}
