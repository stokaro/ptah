package devclean

import (
	"errors"
	"fmt"
	"strings"

	"ptah.run/catalog"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/replaytrust"
)

// ErrUnconfinedRoles is the refusal [RefuseUnconfinedRoles] returns, wrapped
// with the roles it names and the remedies.
var ErrUnconfinedRoles = errors.New("a role belongs to the server, not to the dev database, and outlives the run's cleanup")

// RefuseUnconfinedRoles refuses to create roles, the roles a desired state
// declares that the dev server reached through info does not hold, unless the
// run owns that server or trusts the SQL it executes there. It is the rule the
// replay guard holds a CREATE ROLE in a migration to, for the path that writes
// a desired state on the dev database from its model rather than replaying
// SQL: a role-bootstrap DO block and a bare CREATE ROLE in a desired-state SQL
// file are both a declared role there.
//
// On a server the run owns, a docker URL's or one declared disposable, the
// role is created and the realm cleanup drops it with the run. On a server the
// run only trusts the SQL on, the role is created and stays on the server,
// since trust never widens what the cleanup drops; see [replaytrust]. On any
// other server nothing would drop it, so the refusal comes before the dev
// database is reset or written, and it names each role and the remedies.
//
// A role the server already holds is not passed here: it is left as the
// server has it, and creating nothing changes nothing.
func RefuseUnconfinedRoles(info catalog.ServerInfo, roles []string) error {
	if len(roles) == 0 || DevReplayRealm(info).ownsServer() || replaytrust.Trusted(info.URL) {
		return nil
	}
	quoted := make([]string, 0, len(roles))
	for _, role := range roles {
		quoted = append(quoted, fmt.Sprintf("%q", role))
	}
	noun, pronoun := "role", "it"
	if len(roles) > 1 {
		noun, pronoun = "roles", "them"
	}
	return fmt.Errorf("the desired schema declares %s %s, which the dev server does not hold, and creating %s there "+
		"cannot be confined to the dev database realm: %w; %s; %s",
		noun, strings.Join(quoted, ", "), pronoun, ErrUnconfinedRoles, devdocker.OwnedServerRemedy, replaytrust.Remedy)
}
