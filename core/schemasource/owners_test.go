package schemasource_test

import (
	"ptah.run/core/yamlext"
	"ptah.run/sourceformats"
)

// noOwners reads each output format and selects no feature owner: these
// tests read the frontends' own keys.
var noOwners = sourceformats.New(yamlext.None())
