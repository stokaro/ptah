package yamlschema_test

import (
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/yamlext"
	"ptah.run/internal/pgpolicysource"
)

// noOwners selects no feature owner: these tests read the frontend's own keys,
// and an owner's claims are tested beside the owner.
var noOwners = yamlext.None()

// rowSecurityOwners selects the row-security owner alone, so a test reads
// PostgreSQL row-level security the way a runtime that registers the owner
// does.
var rowSecurityOwners = must.Must(yamlext.NewSet(pgpolicysource.YAML()))
