package providerbundle_test

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"
	"go.yaml.in/yaml/v3"

	"ptah.run/internal/providercatalog"
)

type releaseConfig struct {
	Before struct {
		Hooks []string `yaml:"hooks"`
	} `yaml:"before"`
	Builds []struct {
		ID     string   `yaml:"id"`
		GOOS   []string `yaml:"goos"`
		GOARCH []string `yaml:"goarch"`
	} `yaml:"builds"`
	Archives []struct {
		Files []any `yaml:"files"`
	} `yaml:"archives"`
	Dockers []struct {
		ExtraFiles []string `yaml:"extra_files"`
	} `yaml:"dockers_v2"`
}

func loadReleaseConfig(c *qt.C) releaseConfig {
	data := must.Must(os.ReadFile(filepath.Join("..", "..", ".goreleaser.yaml")))
	var config releaseConfig
	c.Assert(yaml.Unmarshal(data, &config), qt.IsNil)
	return config
}

// Every platform the host builds has a pinned provider artifact, and the pin
// names no platform the host does not build. A new host platform without a
// pinned artifact fails here, so adding it is a decision: pin it, or state the
// platform as unsupported by the bundle in the documentation.
func TestGoReleaser_HostPlatformsMatchThePin(t *testing.T) {
	c := qt.New(t)
	config := loadReleaseConfig(c)
	var built []string
	for _, build := range config.Builds {
		for _, goos := range build.GOOS {
			for _, goarch := range build.GOARCH {
				built = append(built, goos+"/"+goarch)
			}
		}
	}
	slices.Sort(built)
	built = slices.Compact(built)
	var pinned []string
	for _, platform := range providercatalog.Platforms() {
		pinned = append(pinned, platform.String())
	}
	slices.Sort(pinned)
	c.Assert(built, qt.DeepEquals, pinned)
}

// The staging command runs before the build, the archives take the staged
// directory under the name the host looks for, and the image takes the Linux
// platforms.
func TestGoReleaser_ShipsTheStagedProviders(t *testing.T) {
	c := qt.New(t)
	config := loadReleaseConfig(c)
	c.Assert(config.Before.Hooks, qt.Contains, "go run ./internal/cmd/providerbundle --out .release-providers")

	var archiveFiles []string
	for _, archive := range config.Archives {
		for _, file := range archive.Files {
			archiveFiles = append(archiveFiles, fmt.Sprint(file))
		}
	}
	c.Assert(archiveFiles, qt.Contains, "map[dst:providers src:.release-providers/{{ .Os }}_{{ .Arch }}/providers]")

	c.Assert(config.Dockers, qt.HasLen, 1)
	c.Assert(config.Dockers[0].ExtraFiles, qt.Contains, ".release-providers/linux_amd64")
	c.Assert(config.Dockers[0].ExtraFiles, qt.Contains, ".release-providers/linux_arm64")
	dockerfile := string(must.Must(os.ReadFile(filepath.Join("..", "..", "Dockerfile"))))
	c.Assert(dockerfile, qt.Contains, "COPY .release-providers/${TARGETOS}_${TARGETARCH}/providers /usr/local/bin/providers")
}
