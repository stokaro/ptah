package goschema_test

import (
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/annotation"
	"ptah.run/internal/pgpolicysource"
)

// noOwners selects no feature owner: these tests read the frontend's own
// directives, and an owner's are tested beside the owner.
var noOwners = annotation.None()

// rowSecurityOwners selects the row-security owner alone, so a test reads
// PostgreSQL row-level security the way a runtime that registers the owner
// does.
var rowSecurityOwners = must.Must(annotation.NewSet(pgpolicysource.Annotations()))
