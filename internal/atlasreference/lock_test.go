package atlasreference_test

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasreference"
)

// repositoryRoot is where the lock and the Renovate policy live, relative to
// this package. The lock decides what CI builds; Release decides what every
// reference suite accepts.
const repositoryRoot = "../.."

const (
	lockFile     = "scripts/atlas-ce-reference.lock"
	constantFile = "internal/atlasreference/atlasreference.go"
)

// The dependency both Renovate managers must name. Renovate names a branch
// after the dependency, so two managers that agree on it move the lock and the
// constant in one pull request, and one that does not would open a second pull
// request in which the lock test below fails.
const (
	atlasDepName    = "ariga/atlas"
	atlasDatasource = "github-tags"
)

// TestLock_PinsTheRelease holds the lock and the constant together. A lock
// moved alone builds a binary every reference suite then refuses as the wrong
// version; a constant moved alone makes them refuse the binary CI built.
func TestLock_PinsTheRelease(t *testing.T) {
	c := qt.New(t)

	lock := readLock(c)

	c.Assert(lock["version"], qt.Equals, atlasreference.Release)
}

// TestRenovate_MovesTheAtlasLock runs the lock manager's pattern over the lock,
// with Go's regexp package, which implements the same RE2 syntax Renovate
// evaluates custom managers with.
//
// The rendered replacement must reproduce the matched bytes when it is given
// the current values. Then a replacement with new values changes only the tag
// and the commit, and keeps every line the build script reads in the shape it
// reads them.
func TestRenovate_MovesTheAtlasLock(t *testing.T) {
	c := qt.New(t)
	lock := readLock(c)
	manager := atlasManagerFor(c, lockFile)

	match := matchOnce(c, manager, readRepositoryFile(c, lockFile))

	c.Assert(match["currentValue"], qt.Equals, atlasreference.Release)
	c.Assert(match["currentDigest"], qt.Equals, lock["source_commit"])
	c.Assert(renderReplacement(manager.AutoReplaceStringTemplate, atlasreference.Release, lock["source_commit"]),
		qt.Equals, match[""])
}

// TestRenovate_MovesTheReleaseWithTheLock is the other half: without it the bot
// moves the lock and leaves Release behind, and TestLock_PinsTheRelease fails on
// the bot's own pull request.
func TestRenovate_MovesTheReleaseWithTheLock(t *testing.T) {
	c := qt.New(t)
	manager := atlasManagerFor(c, constantFile)

	match := matchOnce(c, manager, readRepositoryFile(c, constantFile))

	c.Assert(match["currentValue"], qt.Equals, atlasreference.Release)
}

// customManager is one Renovate regex manager, read for the fields these tests
// check.
type customManager struct {
	ManagerFilePatterns       []string `json:"managerFilePatterns"`
	MatchStrings              []string `json:"matchStrings"`
	AutoReplaceStringTemplate string   `json:"autoReplaceStringTemplate"`
	DatasourceTemplate        string   `json:"datasourceTemplate"`
	DepNameTemplate           string   `json:"depNameTemplate"`
}

// atlasManagerFor returns the one custom manager that reads path, and asserts
// it names the Atlas dependency through the shared datasource.
func atlasManagerFor(c *qt.C, path string) customManager {
	c.Helper()

	var config struct {
		CustomManagers []customManager `json:"customManagers"`
	}
	c.Assert(json.Unmarshal([]byte(readRepositoryFile(c, "renovate.json")), &config), qt.IsNil)

	var found []customManager
	for _, manager := range config.CustomManagers {
		if readsFile(c, manager, path) {
			found = append(found, manager)
		}
	}
	c.Assert(found, qt.HasLen, 1, qt.Commentf("want exactly one Renovate custom manager reading %s", path))
	c.Assert(found[0].DepNameTemplate, qt.Equals, atlasDepName)
	c.Assert(found[0].DatasourceTemplate, qt.Equals, atlasDatasource)
	c.Assert(found[0].MatchStrings, qt.HasLen, 1)
	return found[0]
}

// readsFile reports whether one of the manager's file patterns, written in
// Renovate's `/regex/` form, matches path.
func readsFile(c *qt.C, manager customManager, path string) bool {
	c.Helper()

	for _, pattern := range manager.ManagerFilePatterns {
		body, ok := strings.CutPrefix(pattern, "/")
		c.Assert(ok, qt.IsTrue, qt.Commentf("pattern %q is not in /regex/ form", pattern))
		body, ok = strings.CutSuffix(body, "/")
		c.Assert(ok, qt.IsTrue, qt.Commentf("pattern %q is not in /regex/ form", pattern))
		if regexp.MustCompile(body).MatchString(path) {
			return true
		}
	}
	return false
}

// matchOnce applies the manager's pattern and returns its named groups, with
// the whole match under the empty name. A pattern matching twice would let
// Renovate rewrite the wrong occurrence; one matching nothing would leave the
// pin where it is.
func matchOnce(c *qt.C, manager customManager, content string) map[string]string {
	c.Helper()

	pattern := regexp.MustCompile(manager.MatchStrings[0])
	matches := pattern.FindAllStringSubmatch(content, -1)
	c.Assert(matches, qt.HasLen, 1, qt.Commentf("pattern %q", pattern))

	groups := make(map[string]string)
	for i, name := range pattern.SubexpNames() {
		groups[name] = matches[0][i]
	}
	return groups
}

// renderReplacement expands the two Handlebars variables a replacement
// template may use.
func renderReplacement(template, value, digest string) string {
	return strings.NewReplacer("{{{newValue}}}", value, "{{{newDigest}}}", digest).Replace(template)
}

// readLock returns the lock's key-value lines, read the way
// scripts/lib/atlas-ce-reference.sh reads them: blank lines and comments
// skipped, one key and one value per line.
func readLock(c *qt.C) map[string]string {
	c.Helper()

	fields := make(map[string]string)
	scanner := bufio.NewScanner(strings.NewReader(readRepositoryFile(c, lockFile)))
	for scanner.Scan() {
		key, value, _ := strings.Cut(strings.TrimSpace(scanner.Text()), " ")
		if key == "" || strings.HasPrefix(key, "#") {
			continue
		}
		fields[key] = value
	}
	c.Assert(scanner.Err(), qt.IsNil)
	return fields
}

func readRepositoryFile(c *qt.C, path string) string {
	c.Helper()

	content, err := os.ReadFile(filepath.Join(repositoryRoot, filepath.FromSlash(path)))
	c.Assert(err, qt.IsNil)
	return string(content)
}
