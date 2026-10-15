package mysqllint

import "ptah.run/core/platform"

// family is the pair Ptah's MySQL rules are defined for. Each call returns a
// new slice, so no rule's targets alias another's.
func family() []string {
	return []string{platform.MySQL, platform.MariaDB}
}
