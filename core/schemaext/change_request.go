package schemaext

import (
	"context"
	"slices"

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
// accepts its action and implements [WrittenResolver], refuses one that none
// of them resolves or that two resolve, and compares the request as the one
// resolution names it, so a misspelled object cannot read as a change made.
type ChangeRequest struct {
	Subject objectidentity.ID
	Action  string
	Written string
}

// RotateAction asks the owner of an object to give it the value its
// declaration holds now. No comparison finds such a change for an object whose
// value the server never returns, as YDB never returns a secret's.
const RotateAction = "rotate"

// WrittenResolver is implemented by an [ObjectComparisonService] that reads the
// objects of its kinds in the spelling an operator writes, such as a secret's
// path. The runtime asks it before the comparison runs; see
// [ChangeRequest.Written].
type WrittenResolver interface {
	// ResolveWritten returns the object written names for action, and false
	// when the spelling names no object of the service's kinds. The runtime
	// asks only for an action the service's comparison accepts. An error
	// refuses the request and says what is wrong with the spelling. It reads
	// no schema state: whether the desired state declares the object is the
	// comparison's to answer.
	ResolveWritten(ctx context.Context, action, written string) (objectidentity.ID, bool, error)
}

// WrittenRequests returns a request for action on each object written names,
// in order and each spelling once.
func WrittenRequests(action string, written []string) []ChangeRequest {
	requests := make([]ChangeRequest, 0, len(written))
	for _, spelling := range written {
		if slices.ContainsFunc(requests, func(held ChangeRequest) bool { return held.Written == spelling }) {
			continue
		}
		requests = append(requests, ChangeRequest{Action: action, Written: spelling})
	}
	return requests
}
