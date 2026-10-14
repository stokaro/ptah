package pgdefaultprivilege

import (
	"strings"
)

// String describes each changed right and grant option in a deterministic
// order. It does not modify the change. A nil or invalid change reports an
// invalid delta; a restoration reports the captured grants and revocations.
func (v *Change) String() string {
	if v.Validate() != nil {
		return "invalid default privilege change"
	}
	if v.Restore != nil {
		restored := v.Restore.Canonical()
		return "restore grants [" + describeGrants(restored.Privileges) + "]; revoked [" + strings.Join(restored.Revoked, ", ") + "]"
	}
	var parts []string
	for _, section := range []struct{ label, names string }{
		{"grant", describeGrants(canonicalGrants(v.Granted))},
		{"revoke", describeGrants(canonicalGrants(v.Revoked))},
		{"grant option", strings.Join(canonicalNames(v.GrantOptionsAdded), ", ")},
		{"revoke grant option", strings.Join(canonicalNames(v.GrantOptionsRevoked), ", ")},
	} {
		if section.names != "" {
			parts = append(parts, section.label+" "+section.names)
		}
	}
	return strings.Join(parts, "; ")
}

func describeGrants(grants []Grant) string {
	names := make([]string, 0, len(grants))
	for _, grant := range grants {
		name := grant.Privilege
		if grant.WithOption {
			name += " WITH GRANT OPTION"
		}
		names = append(names, name)
	}
	return strings.Join(names, ", ")
}
