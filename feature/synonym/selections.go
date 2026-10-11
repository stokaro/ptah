package synonym

import (
	"context"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/core/selectext"
)

// Selections is how a command's scope names a synonym: by its own name, the
// alias, never its target's, in the schema that holds it. The alias is a
// relation's name, so excluding the synonym excludes a table of its name
// too. An exclusion may subtract a declared synonym's comment, which keeps
// the synonym and leaves the comment out: `*[type=synonym].comment`.
func Selections() selectext.Extension {
	return selectext.Extension{Owner: Owner, Selections: []selectext.Selection{
		{Kind: Kind, Type: "synonym", Naming: selectext.InSchema, Relation: true, Fields: []string{"comment"}},
	}, Service: selectionService{}}
}

// selectionService clears the fields an exclusion subtracts.
type selectionService struct{}

// SelectObjects clears the comment of each declared synonym an exclusion
// names; an observed synonym carries none. A synonym requires nothing.
func (selectionService) SelectObjects(_ context.Context, request selectext.Request) (selectext.Result, error) {
	var result selectext.Result
	for _, removal := range request.Clears {
		object, held, err := request.Kept.Get(removal.Object)
		if err != nil || !held {
			return selectext.Result{}, fmt.Errorf("%w: synonym %s to clear is not kept", schemaext.ErrInvalidValue, removal.Object)
		}
		declared, ok := object.Value.(*DesiredSynonym)
		if !ok {
			continue
		}
		cleared := *declared
		cleared.Comment = ""
		object.Value = &cleared
		result.Cleared = append(result.Cleared, object)
	}
	return result, nil
}
