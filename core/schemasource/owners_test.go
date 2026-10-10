package schemasource_test

import "ptah.run/core/yamlext"

// noOwners selects no feature owner for the output formats that have owners:
// these tests read the frontends' own keys.
type noOwners struct{}

func (noOwners) YAML() yamlext.Set { return yamlext.None() }
