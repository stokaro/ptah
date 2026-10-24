package awsparamstore_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"go.yaml.in/yaml/v3"
)

// ssmClientPrefix is the import path of the SDK's SSM client and its
// subpackages, schemas among them.
const ssmClientPrefix = "github.com/aws/aws-sdk-go-v2/service/ssm"

// shippedPrograms reads the main packages .goreleaser.yaml builds, the
// programs a release ships.
func shippedPrograms(c *qt.C) []string {
	root := strings.TrimSpace(goList(c, "-m", "-f", "{{.Dir}}"))
	raw, err := os.ReadFile(filepath.Join(root, ".goreleaser.yaml"))
	c.Assert(err, qt.IsNil)
	var release struct {
		Builds []struct {
			Main string `yaml:"main"`
		} `yaml:"builds"`
	}
	c.Assert(yaml.Unmarshal(raw, &release), qt.IsNil)
	programs := make([]string, 0, len(release.Builds))
	for _, build := range release.Builds {
		programs = append(programs, "ptah.run/"+strings.TrimPrefix(build.Main, "./"))
	}
	return programs
}

func goList(c *qt.C, args ...string) string {
	output, err := exec.Command("go", append([]string{"list"}, args...)...).Output() // #nosec G204 -- the arguments are package paths this test names
	c.Assert(err, qt.IsNil)
	return string(output)
}

// ssmPackagesLinkedBy lists the SSM client packages the given packages link.
func ssmPackagesLinkedBy(c *qt.C, packages ...string) []string {
	linked := strings.Fields(goList(c, append([]string{"-deps", "-f", "{{.ImportPath}}"}, packages...)...))
	return slices.DeleteFunc(linked, func(path string) bool {
		return path != ssmClientPrefix && !strings.HasPrefix(path, ssmClientPrefix+"/")
	})
}

// TestShippedProgramsLinkNoSSMClient holds the reason this package exists:
// no shipped program links the SDK's SSM client, whose schema package keeps
// about 1.4 MB live in every process from initialization on.
//
// The control lists the Go CDK driver's dependencies through the same
// matcher, so a renamed SDK path fails the control rather than leaving the
// invariant guarding nothing.
func TestShippedProgramsLinkNoSSMClient(t *testing.T) {
	c := qt.New(t)
	programs := shippedPrograms(c)
	c.Assert(programs, qt.Contains, "ptah.run/cmd/ptah")
	c.Assert(programs, qt.Contains, "ptah.run/cmd/ptah-compat")

	c.Assert(ssmPackagesLinkedBy(c, "gocloud.dev/runtimevar/awsparamstore"), qt.Contains, ssmClientPrefix+"/schemas")
	c.Assert(ssmPackagesLinkedBy(c, programs...), qt.HasLen, 0)
}
