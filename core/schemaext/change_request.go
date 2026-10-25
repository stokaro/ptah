package schemaext

import (
	"slices"
	"strings"

	"ptah.run/core/objectidentity"
)

// ChangeRequest asks the owner of Subject's kind for a change that no
// comparison can find by itself, such as a new value for an object whose value
// the server never returns. It belongs to one comparison and is part of
// neither schema state, so no source format can store it and it cannot reach a
// later comparison by accident.
//
// Action names the change. An owner declares the actions it accepts when it
// registers its comparison, and the runtime refuses a request no owner
// accepts, so a request is never ignored. The owner decides what the request
// means for the states it compares; it may also refuse a request, for example
// one that names an object the desired state does not declare.
//
// Written names the object as the operator wrote it, in the engine's own
// spelling, and is set instead of Subject: only the owner reads that spelling.
// The runtime offers a written request to every comparison of the target that
// accepts its action. A comparison that reads the spelling answers it once in
// [ObjectComparisonResult.Resolved] with the desired object it names, and acts
// on it as on a request with that Subject. The runtime refuses a written
// request that no comparison resolves or that two resolve, so a misspelled
// object cannot read as a change made.
type ChangeRequest struct {
	Subject objectidentity.ID
	Action  string
	Written string
}

// ResolvedRequest says which desired object a written [ChangeRequest] names.
// Request is the index of the request in the Requests of the comparison
// request that carried it.
type ResolvedRequest struct {
	Request int
	Subject objectidentity.ID
}

// RotateAction asks the owner of an object to give it the value its
// declaration holds now. No comparison finds such a change for an object whose
// value the server never returns, as YDB never returns a secret's.
const RotateAction = "rotate"

// WrittenRequests returns a request for action on each object written names,
// in order and each spelling once. Surrounding space is not part of a
// spelling.
func WrittenRequests(action string, written []string) []ChangeRequest {
	requests := make([]ChangeRequest, 0, len(written))
	for _, spelling := range written {
		spelling = strings.TrimSpace(spelling)
		if slices.ContainsFunc(requests, func(held ChangeRequest) bool { return held.Written == spelling }) {
			continue
		}
		requests = append(requests, ChangeRequest{Action: action, Written: spelling})
	}
	return requests
}
