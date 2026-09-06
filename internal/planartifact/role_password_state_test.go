package planartifact_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"ptah.run/catalog"
	"ptah.run/internal/planartifact"
)

func TestNewReportRolePasswordStatesProduceDistinctFingerprints(t *testing.T) {
	c := qt.New(t)
	unknown := rolePasswordStateDigest(c, catalog.RolePasswordUnknown)
	absent := rolePasswordStateDigest(c, catalog.RolePasswordAbsent)
	present := rolePasswordStateDigest(c, catalog.RolePasswordPresent)

	c.Assert(unknown, qt.Not(qt.Equals), absent)
	c.Assert(unknown, qt.Not(qt.Equals), present)
	c.Assert(absent, qt.Not(qt.Equals), present)
}

func rolePasswordStateDigest(c *qt.C, state catalog.RolePasswordState) string {
	c.Helper()
	report, err := planartifact.NewReport(
		ocispec.Descriptor{
			Digest:    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			MediaType: ocispec.MediaTypeImageManifest,
			Size:      1,
		},
		&catalog.Database{Roles: []catalog.Role{{
			Name:          "app_user",
			PasswordState: state,
		}}},
		"postgres",
		nil,
		nil,
		nil,
	)
	c.Assert(err, qt.IsNil)
	return report.CurrentSchemaDigest
}
