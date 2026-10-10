package goschema_test

import (
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/annotation"
	"ptah.run/internal/mssqlpolicysource"
	"ptah.run/internal/pgpolicysource"
)

// noOwners selects no feature owner: these tests read the frontend's own
// directives, and an owner's are tested beside the owner.
var noOwners = annotation.None()

// rowSecurityOwners selects the owners that read row-level security by its
// target scope, PostgreSQL's and SQL Server's, so a test reads it the way a
// runtime that registers them does.
var rowSecurityOwners = must.Must(annotation.NewSet(pgpolicysource.Annotations(), mssqlpolicysource.Annotations()))
