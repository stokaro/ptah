package atlas_test

import (
	"testing"
)

// applyWithoutDevURLEnvVar restores planning a non-database desired state with
// no dev database at all. It is spelled out here rather than imported because
// this is the external test package; cmd/atlas/schema_apply.go owns the name and
// documents why the capability is kept behind an environment variable.
const applyWithoutDevURLEnvVar = "PTAH_ATLAS_APPLY_WITHOUT_DEV_URL"

// allowSchemaApplyWithoutDevURL keeps a test on the pre-stokaro/ptah#940 apply
// path, where `schema apply --to file://…` planned without a dev database.
//
// It marks a test that predates the `--dev-url cannot be empty` gate and is not
// about the dev database: the subject is a format template, a lock note, a
// scope selector, or an atlas.hcl env. The gate itself is covered by
// TestSchemaApplyRequiresDevURLForFileSource and its neighbors, which run
// without this.
func allowSchemaApplyWithoutDevURL(t *testing.T) {
	t.Helper()
	t.Setenv(applyWithoutDevURLEnvVar, "1")
}
