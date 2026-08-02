package ocireferrers_test

import (
	"bytes"
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"go.5x5.cz/ptah/internal/ociartifact"
	"go.5x5.cz/ptah/internal/ocireferrers"
)

func TestArtifactType_HappyPath(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name   string
		filter string
		want   string
	}{
		{name: "all", filter: "all", want: ""},
		{name: "lint", filter: "lint", want: ociartifact.LintArtifactType},
		{name: "plan", filter: " PLAN ", want: ociartifact.PlanArtifactType},
		{name: "deployment", filter: "deployment", want: ociartifact.DeploymentArtifactType},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			got, err := ocireferrers.ArtifactType(tt.filter)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, tt.want)
		})
	}
}

func TestArtifactType_FailurePath(t *testing.T) {
	c := qt.New(t)

	_, err := ocireferrers.ArtifactType("schema")

	c.Assert(err, qt.ErrorMatches, `unsupported referrer type "schema": expected all, lint, plan, or deployment`)
}

func TestNewRecords_SortsAndClonesDescriptors(t *testing.T) {
	c := qt.New(t)
	annotations := map[string]string{"org.opencontainers.image.title": "plan.json"}
	descriptors := []ocispec.Descriptor{
		{
			MediaType:    ocispec.MediaTypeImageManifest,
			Digest:       "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
			Size:         303,
			ArtifactType: ociartifact.PlanArtifactType,
			Annotations:  annotations,
		},
		{
			MediaType:    ocispec.MediaTypeImageManifest,
			Digest:       "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			Size:         202,
			ArtifactType: ociartifact.LintArtifactType,
		},
	}

	got := ocireferrers.NewRecords(descriptors)
	annotations["org.opencontainers.image.title"] = "mutated"

	c.Assert(got, qt.HasLen, 2)
	c.Assert(got[0].ArtifactType, qt.Equals, ociartifact.LintArtifactType)
	c.Assert(got[1].ArtifactType, qt.Equals, ociartifact.PlanArtifactType)
	c.Assert(got[1].Annotations["org.opencontainers.image.title"], qt.Equals, "plan.json")
}

func TestWriteJSON_EmptyArrayAndStableFields(t *testing.T) {
	c := qt.New(t)
	var empty bytes.Buffer

	err := ocireferrers.Write(&empty, "json", nil)

	c.Assert(err, qt.IsNil)
	c.Assert(empty.String(), qt.Equals, "[]\n")

	records := []ocireferrers.Record{{
		Digest:       "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		ArtifactType: ociartifact.LintArtifactType,
		MediaType:    ocispec.MediaTypeImageManifest,
		Size:         202,
	}}
	var output bytes.Buffer
	err = ocireferrers.Write(&output, "json", records)
	c.Assert(err, qt.IsNil)
	var got []ocireferrers.Record
	c.Assert(json.Unmarshal(output.Bytes(), &got), qt.IsNil)
	c.Assert(got, qt.DeepEquals, records)
}

func TestWriteText(t *testing.T) {
	c := qt.New(t)
	var output bytes.Buffer
	records := []ocireferrers.Record{{
		Digest:       "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		ArtifactType: ociartifact.LintArtifactType,
		MediaType:    ocispec.MediaTypeImageManifest,
		Size:         202,
	}}

	err := ocireferrers.Write(&output, "text", records)

	c.Assert(err, qt.IsNil)
	c.Assert(output.String(), qt.Contains, "DIGEST\tARTIFACT TYPE\tMEDIA TYPE\tSIZE")
	c.Assert(output.String(), qt.Contains, ociartifact.LintArtifactType)
}

func TestWriteText_EscapesRegistryControlledFields(t *testing.T) {
	c := qt.New(t)
	var output bytes.Buffer
	records := []ocireferrers.Record{{
		Digest:       "sha256:abc\nforged",
		ArtifactType: "lint\x1b]8;;https://example.invalid\a",
		MediaType:    "application/json\tforged",
		Size:         202,
	}}

	err := ocireferrers.Write(&output, "text", records)

	c.Assert(err, qt.IsNil)
	c.Assert(
		output.String(),
		qt.Equals,
		"DIGEST\tARTIFACT TYPE\tMEDIA TYPE\tSIZE\n"+
			"sha256:abc\\nforged\tlint\\x1b]8;;https://example.invalid\\a\tapplication/json\\tforged\t202\n",
	)
}

func TestWrite_FailurePath(t *testing.T) {
	c := qt.New(t)

	err := ocireferrers.Write(&bytes.Buffer{}, "yaml", nil)

	c.Assert(err, qt.ErrorMatches, `unsupported output format "yaml": expected text or json`)
}
