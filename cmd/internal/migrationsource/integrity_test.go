package migrationsource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"go.5x5.cz/ptah/cmd/internal/migrationsource"
	"go.5x5.cz/ptah/migration/migrator"
)

// These rows moved here from cmd/migrateup, where they were a white-box test of
// a private mutableTagSumWarning. The predicate became exported when
// `migrations down` and `status` gained --verify-sum (stokaro/ptah#928 item 4)
// and needed the same provenance qualifier: leaving it private in `up` would
// have meant each new verb reimplementing the sentence, which is the shape that
// let `down` go ungated in the first place. With the predicate exported the
// white-box justification no longer holds, so the rows are black-box now.

const (
	provenanceDigest      = "sha256:" + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	provenanceOtherDigest = "sha256:" + "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

func ociTagSource(reference, tag, resolved string) migrationsource.Source {
	return migrationsource.Source{
		Display:   reference,
		DirFormat: migrator.MigrationDirFormatPtah,
		OCI: &migrationsource.OCI{
			Reference:       reference,
			Descriptor:      ocispec.Descriptor{Digest: digest.Digest(resolved)},
			Tag:             tag,
			DigestReference: "oci://reg.test/ptah/app@" + resolved,
		},
	}
}

func ociDigestSource(resolved string) migrationsource.Source {
	reference := "oci://reg.test/ptah/app@" + resolved
	return migrationsource.Source{
		Display:   reference,
		DirFormat: migrator.MigrationDirFormatPtah,
		OCI: &migrationsource.OCI{
			Reference:       reference,
			Descriptor:      ocispec.Descriptor{Digest: digest.Digest(resolved)},
			PinnedByDigest:  true,
			DigestReference: reference,
		},
	}
}

func localSource() migrationsource.Source {
	return migrationsource.Source{
		Display:   "/srv/app/migrations",
		DirFormat: migrator.MigrationDirFormatPtah,
	}
}

// TestMutableTagSumWarning pins which provenances get the qualifier. Only the
// tag rows may produce text: a digest reference already names the exact bytes,
// a local directory carries a sum reviewed beside the migrations, and an
// unhashed source verified nothing, so it claimed nothing to qualify.
func TestMutableTagSumWarning(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name            string
		source          func() migrationsource.Source
		verifiedSumFile string
		want            string
	}{
		{
			name: "oci tag verified names tag digest and pin",
			source: func() migrationsource.Source {
				return ociTagSource("oci://reg.test/ptah/app:release", "release", provenanceDigest)
			},
			verifiedSumFile: "ptah.sum",
			want: "oci://reg.test/ptah/app:release is a movable tag: ptah.sum travels inside the artifact, " +
				"so verifying it proves the pulled files are internally consistent, not that they are the " +
				"reviewed ones. This tag resolved to " + provenanceDigest + "; pass oci://reg.test/ptah/app@" +
				provenanceDigest + " to pin these exact bytes.",
		},
		{
			name:            "oci digest verified stays silent",
			source:          func() migrationsource.Source { return ociDigestSource(provenanceDigest) },
			verifiedSumFile: "ptah.sum",
			want:            "",
		},
		{
			name:            "local directory verified stays silent",
			source:          localSource,
			verifiedSumFile: "ptah.sum",
			want:            "",
		},
		{
			name: "oci tag with nothing verified stays silent",
			source: func() migrationsource.Source {
				return ociTagSource("oci://reg.test/ptah/app:release", "release", provenanceDigest)
			},
			verifiedSumFile: "",
			want:            "",
		},
		{
			name: "oci tag quotes the digest it actually resolved to",
			source: func() migrationsource.Source {
				return ociTagSource("oci://reg.test/ptah/app:release", "release", provenanceOtherDigest)
			},
			verifiedSumFile: "ptah.sum",
			want: "oci://reg.test/ptah/app:release is a movable tag: ptah.sum travels inside the artifact, " +
				"so verifying it proves the pulled files are internally consistent, not that they are the " +
				"reviewed ones. This tag resolved to " + provenanceOtherDigest + "; pass oci://reg.test/ptah/app@" +
				provenanceOtherDigest + " to pin these exact bytes.",
		},
		{
			name: "oci tag names the sum file that actually verified",
			source: func() migrationsource.Source {
				return ociTagSource("oci://reg.test/ptah/app:stable", "stable", provenanceDigest)
			},
			verifiedSumFile: "atlas.sum",
			want: "oci://reg.test/ptah/app:stable is a movable tag: atlas.sum travels inside the artifact, " +
				"so verifying it proves the pulled files are internally consistent, not that they are the " +
				"reviewed ones. This tag resolved to " + provenanceDigest + "; pass oci://reg.test/ptah/app@" +
				provenanceDigest + " to pin these exact bytes.",
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			c.Assert(migrationsource.MutableTagSumWarning(tt.source(), tt.verifiedSumFile), qt.Equals, tt.want)
		})
	}
}

// TestVerifySumQualifier_DoesNotPromiseAuthenticityFromADigest pins the half of
// the sentence that over-claimed in the opposite direction.
//
// The qualifier exists so `--verify-sum` does not read as tamper detection. Its
// closing clause used to say "pin a digest for authenticity", which fixes the
// first over-claim by making a second one: a digest identifies exact bytes and
// says nothing about who produced them. An attacker able to repoint a tag makes
// the command resolve, display, and then pin THEIR digest — reproducibly
// installing the attacker's bytes. docs/oci_registry.md has said this under
// "Identity, integrity, and authenticity" all along, so the help contradicted
// the repository's own security section.
//
// The banned phrases are the spellings that actually appeared, not a guess at
// every possible one; this is a regression guard, not a prose linter. The
// positive half is what stops the guard being satisfied by deleting the clause
// and saying nothing at all.
func TestVerifySumQualifier_DoesNotPromiseAuthenticityFromADigest(t *testing.T) {
	banned := []struct {
		name   string
		phrase string
	}{
		{name: "the original over-claim", phrase: "pin a digest for authenticity"},
		{name: "the same claim, reworded", phrase: "digest for authenticity"},
	}

	for _, tt := range banned {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(migrationsource.VerifySumQualifier, qt.Not(qt.Contains), tt.phrase)
		})
	}

	required := []struct {
		name   string
		phrase string
	}{
		{name: "says what pinning does give", phrase: "fixes which bytes"},
		{name: "says what it does not give", phrase: "does not establish who published them"},
		{name: "names a control that does", phrase: "signature policy"},
	}

	for _, tt := range required {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(migrationsource.VerifySumQualifier, qt.Contains, tt.phrase)
		})
	}
}

// TestVerifySumUsage_EndsWithTheSharedQualifier pins the composition rule the
// flag help depends on.
//
// The per-verb lead differs because what --verify-sum ADDS differs per verb;
// the qualifier does not, and it has to be last so it reads as the caveat on
// everything before it. cmd/root's flag-surface gate asserts that every
// registered --verify-sum help carries the qualifier; this asserts the helper
// those registrations go through cannot produce one that does not.
func TestVerifySumUsage_EndsWithTheSharedQualifier(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name string
		lead string
	}{
		{name: "up-style lead", lead: "Require a sum file"},
		{name: "status-style lead", lead: "Verify the migration directory before reporting"},
		{name: "empty lead", lead: ""},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			usage := migrationsource.VerifySumUsage(tt.lead)
			c.Assert(usage, qt.Contains, migrationsource.VerifySumQualifier)
			c.Assert(usage[len(usage)-len(migrationsource.VerifySumQualifier):],
				qt.Equals, migrationsource.VerifySumQualifier)
		})
	}
}
