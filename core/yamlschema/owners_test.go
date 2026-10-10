package yamlschema_test

import "ptah.run/core/yamlext"

// noOwners selects no feature owner: these tests read the frontend's own keys,
// and an owner's claims are tested beside the owner.
var noOwners = yamlext.None()
