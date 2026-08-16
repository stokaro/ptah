package dbtarget_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/dbtarget"
)

// An integration test that reads a database variable itself knows only the
// spellings whoever wrote it happened to list. Seven PostgreSQL helpers did,
// and a checkout configured with POSTGRES_TEST_URL -- a spelling this registry
// declares -- skipped all seven while reporting passing packages
// (stokaro/ptah#1541). A skip reads as a pass, so nothing was red.
//
// The names come from the registry rather than from a list here, so a synonym
// added there is covered without anyone remembering to add it twice.
//
// Variables the registry does not own are not this gate's business: the
// destructive MYSQL_CLEANUP_TEST_DSN target and the oracle-specific
// PTAH_ATLAS_ORACLE_POSTGRES_DEV_URL are deliberately separate, which is why
// this needs no allowlist -- neither name is in the registry to begin with.
func TestNoIntegrationTestReadsARegistryVariableDirectly(t *testing.T) {
	c := qt.New(t)
	files := integrationTestFiles(c)
	// A scanner that selects nothing is also green, and that is the failure
	// this gate is most likely to develop.
	c.Assert(len(files) > 100, qt.IsTrue, qt.Commentf("scanned %d files", len(files)))

	offenders := readsRegistryVariable(c, files, registryVariables())

	c.Assert(offenders, qt.HasLen, 0)
}

// The gate has to see a violation to be worth anything, so the same scan is
// pointed at a file that commits one.
func TestNoIntegrationTestReadsARegistryVariableDirectlySelfTest(t *testing.T) {
	c := qt.New(t)
	planted := filepath.Join(c.TempDir(), "planted_live_test.go")
	source := "package x\n\nfunc f() string { return os.Getenv(\"POSTGRES_TEST_DSN\") }\n"
	c.Assert(os.WriteFile(planted, []byte(source), 0o600), qt.IsNil)

	offenders := readsRegistryVariable(c, []string{planted}, registryVariables())

	c.Assert(offenders, qt.HasLen, 1)
}

func registryVariables() []string {
	var names []string
	for _, engine := range dbtarget.Engines() {
		names = append(names, dbtarget.Variables(engine)...)
	}
	return names
}

// readsRegistryVariable reports the files that name a registry variable beside
// an environment read. It looks for the pair rather than for the name alone,
// because a test may legitimately mention a variable in a comment or set one
// for a subprocess it launches.
func readsRegistryVariable(c *qt.C, files, names []string) []string {
	c.Helper()
	var offenders []string
	for _, path := range files {
		contents, err := os.ReadFile(path)
		c.Assert(err, qt.IsNil)
		for line := range strings.SplitSeq(string(contents), "\n") {
			offenders = appendIfDirectRead(offenders, path, line, names)
		}
	}
	return offenders
}

func appendIfDirectRead(offenders []string, path, line string, names []string) []string {
	if !strings.Contains(line, "os.Getenv(") {
		return offenders
	}
	for _, name := range names {
		if strings.Contains(line, `"`+name+`"`) {
			return append(offenders, path+": "+strings.TrimSpace(line))
		}
	}
	return offenders
}

// integrationTestFiles lists the test sources under integration/.
//
// It walks that subtree rather than the repository root on purpose: a linked
// git worktree parked under the repository is an ordinary directory to a walk,
// and walking from the root judges every checkout sharing the parent. Nothing
// parks a worktree inside integration/.
func integrationTestFiles(c *qt.C) []string {
	c.Helper()
	var files []string
	root := filepath.Join(repositoryRoot(c), "integration")
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		files = append(files, path)
		return nil
	})
	c.Assert(err, qt.IsNil)
	return files
}

func repositoryRoot(c *qt.C) string {
	c.Helper()
	wd, err := os.Getwd()
	c.Assert(err, qt.IsNil)
	// The test runs in internal/dbtarget.
	root := filepath.Dir(filepath.Dir(wd))
	_, err = os.Stat(filepath.Join(root, "go.mod"))
	c.Assert(err, qt.IsNil)
	return root
}
