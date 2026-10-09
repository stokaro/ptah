package schemaext

import "ptah.run/core/objectidentity"

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
type ChangeRequest struct {
	Subject objectidentity.ID
	Action  string
}
