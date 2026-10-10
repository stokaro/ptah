package goschema_test

import "ptah.run/core/annotation"

// noOwners selects no feature owner: these tests read the frontend's own
// directives, and an owner's are tested beside the owner.
var noOwners = annotation.None()
