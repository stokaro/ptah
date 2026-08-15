// Symbolic links are half of what this file measures, and creating one on
// Windows needs a privilege an ordinary CI account does not have. The build tag
// follows atlas_root_unix_test.go, which is unix-only for the same reason.
//go:build !windows

package projectconfig_test

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/config/projectconfig"
)

// The atlas.hcl file() sandbox, pinned. See stokaro/ptah#1042.
//
// file() inlines the contents of a file into a config value -- a database URL,
// an error message, anything the config can name. The pinned community binary
// resolves that argument against the whole filesystem: an absolute path, a
// parent-traversal path, and a symbolic link out of the config directory all
// read the target and exit 0. Ptah confines it to the directory holding
// atlas.hcl.
//
// That divergence is deliberate and it is measured, not remembered: the
// community half of it is pinned by TestOracleReadsFilesOutsideTheAtlasHCLDirectory
// in atlas_file_sandbox_oracle_test.go, which runs the binary. This file pins
// the Ptah half.
//
// The refusals below are checked through both loaders that reach a real
// directory, because the two build the filesystem the sandbox rests on
// separately, and only one of them was ever symlink-safe: ParseAtlas used
// os.DirFS, which follows a link straight out of the directory.

// outsideFileMarker is the content of every file planted outside the config
// directory. Every refusal is asserted not to contain it, so a message that
// quotes the file it refused to read cannot pass as a refusal.
const outsideFileMarker = "PTAH-1042-SECRET-OUTSIDE-THE-CONFIG-DIRECTORY"

// insideFileMarker is the content of files that legitimately live inside the config
// directory, so a control can assert the value was actually read.
const insideFileMarker = "PTAH-1042-INSIDE-THE-CONFIG-DIRECTORY"

// atlasFileLoader is one way to reach the file() sandbox with a config that
// lives on disk.
type atlasFileLoader struct {
	name string
	load func(c *qt.C, path string) (projectconfig.Config, error)
}

func atlasFileLoaders() []atlasFileLoader {
	return []atlasFileLoader{
		{
			name: "LoadAtlasFile",
			load: func(_ *qt.C, path string) (projectconfig.Config, error) {
				return projectconfig.LoadAtlasFile(path, "local")
			},
		},
		{
			name: "ParseAtlas",
			load: func(c *qt.C, path string) (projectconfig.Config, error) {
				raw, err := os.ReadFile(path)
				c.Assert(err, qt.IsNil)
				return projectconfig.ParseAtlas(raw, path, "local")
			},
		},
	}
}

// writeAtlasFileConfig writes an atlas.hcl whose env URL is the contents of the
// named file, and returns its path. The value lands in a field the caller can
// read back, so a successful read is provable rather than merely not an error.
func writeAtlasFileConfig(c *qt.C, dir, argument string) string {
	c.Helper()

	path := filepath.Join(dir, "atlas.hcl")
	body := "env \"local\" {\n  url = file(" + quoteHCL(argument) + ")\n}\n"
	c.Assert(os.WriteFile(path, []byte(body), 0o600), qt.IsNil)
	return path
}

func quoteHCL(value string) string {
	return "\"" + value + "\""
}

// plantOutsideSecret writes the out-of-directory file every escape below aims
// at, one level above the config directory, and returns its absolute path.
func plantOutsideSecret(c *qt.C, base string) string {
	c.Helper()

	path := filepath.Join(base, "secret.txt")
	c.Assert(os.WriteFile(path, []byte(outsideFileMarker), 0o600), qt.IsNil)
	return path
}

func symlink(c *qt.C, target, link string) {
	c.Helper()

	c.Assert(os.Symlink(target, link), qt.IsNil)
}

// The escape shapes. Each returns the argument to hand file(), after planting
// whatever the shape needs in the config directory.

func escapeAbsolutePath(c *qt.C, _, outside string) string {
	c.Helper()

	return outside
}

func escapeParentTraversal(c *qt.C, _, outside string) string {
	c.Helper()

	return "../" + filepath.Base(outside)
}

func escapeSymlinkedFile(c *qt.C, dir, outside string) string {
	c.Helper()

	symlink(c, outside, filepath.Join(dir, "link.txt"))
	return "link.txt"
}

func escapeSymlinkedRelativeFile(c *qt.C, dir, outside string) string {
	c.Helper()

	symlink(c, filepath.Join("..", filepath.Base(outside)), filepath.Join(dir, "relative.link"))
	return "relative.link"
}

func escapeSymlinkedDirectory(c *qt.C, dir, outside string) string {
	c.Helper()

	symlink(c, filepath.Dir(outside), filepath.Join(dir, "outdir"))
	return "outdir/" + filepath.Base(outside)
}

func escapeSymlinkChain(c *qt.C, dir, outside string) string {
	c.Helper()

	symlink(c, filepath.Join("..", filepath.Base(outside)), filepath.Join(dir, "second.link"))
	symlink(c, "second.link", filepath.Join(dir, "first.link"))
	return "first.link"
}

// escapeLongSymlinkChain builds hop0 -> hop1 -> ... -> the outside file, longer
// than the sandbox's own resolution and well short of any operating system's
// limit on chained links, so the read is attempted rather than refused as a
// loop.
func escapeLongSymlinkChain(c *qt.C, dir, outside string) string {
	c.Helper()

	const hops = 6
	for hop := range hops - 1 {
		symlink(c, hopLinkName(hop+1), filepath.Join(dir, hopLinkName(hop)))
	}
	symlink(c, filepath.Join("..", filepath.Base(outside)), filepath.Join(dir, hopLinkName(hops-1)))
	return hopLinkName(0)
}

func hopLinkName(hop int) string {
	return "hop" + strconv.Itoa(hop) + ".link"
}

// escapeSymlinkReentry leaves the directory and comes back to a file inside it.
// It reads nothing an author could not have named directly, and it is refused
// anyway: the rule is about where the path goes, not about what it ends up on,
// and a rooted filesystem refuses it for exactly the same reason.
func escapeSymlinkReentry(c *qt.C, dir, _ string) string {
	c.Helper()

	inside := filepath.Join(dir, "inside.txt")
	c.Assert(os.WriteFile(inside, []byte(insideFileMarker), 0o600), qt.IsNil)
	symlink(c, filepath.Join("..", filepath.Base(dir), "inside.txt"), filepath.Join(dir, "reentry.link"))
	return "reentry.link"
}

func TestAtlasFileSandboxRefusesReadsOutsideTheConfigDirectory(t *testing.T) {
	tests := []struct {
		name  string
		plant func(c *qt.C, dir, outside string) string
		err   string
	}{
		{
			name:  "absolute path",
			plant: escapeAbsolutePath,
			err:   `.*absolute paths are not supported: .*secret\.txt: atlas\.hcl file\(\) and fileset\(\) read only inside the directory holding atlas\.hcl; pass a value from outside it through getenv\(\).*`,
		},
		{
			name:  "parent traversal",
			plant: escapeParentTraversal,
			err:   `.*path escapes atlas\.hcl directory: \.\./secret\.txt: atlas\.hcl file\(\) and fileset\(\) read only inside the directory holding atlas\.hcl.*`,
		},
		{
			name:  "symbolic link to an absolute path outside",
			plant: escapeSymlinkedFile,
			err:   `.*path escapes atlas\.hcl directory: link\.txt: link\.txt is a symbolic link pointing outside it.*`,
		},
		{
			name:  "symbolic link to a relative path outside",
			plant: escapeSymlinkedRelativeFile,
			err:   `.*path escapes atlas\.hcl directory: relative\.link: relative\.link is a symbolic link pointing outside it.*`,
		},
		{
			name:  "symbolic link to a directory outside",
			plant: escapeSymlinkedDirectory,
			err:   `.*path escapes atlas\.hcl directory: outdir/secret\.txt: outdir is a symbolic link pointing outside it.*`,
		},
		{
			name:  "chain of symbolic links leaving the directory",
			plant: escapeSymlinkChain,
			err:   `.*path escapes atlas\.hcl directory: first\.link: second\.link is a symbolic link pointing outside it.*`,
		},
		{
			name:  "symbolic link that leaves and re-enters",
			plant: escapeSymlinkReentry,
			err:   `.*path escapes atlas\.hcl directory: reentry\.link: reentry\.link is a symbolic link pointing outside it.*`,
		},
		{
			// The row that measures the filesystem rather than the walk. The
			// chain is longer than the sandbox resolves, so the named refusal
			// above is not reached and the only thing left standing is the
			// rooted filesystem the loader opened -- which is why its wording
			// is the operating system's. Hand either loader an os.DirFS instead
			// and this reads the file.
			name:  "chain longer than the sandbox resolves",
			plant: escapeLongSymlinkChain,
			err:   `.*openat hop0\.link: path escapes from parent.*`,
		},
	}

	for _, loader := range atlasFileLoaders() {
		for _, tt := range tests {
			t.Run(loader.name+"/"+tt.name, func(t *testing.T) {
				c := qt.New(t)

				base := t.TempDir()
				dir := filepath.Join(base, "project")
				c.Assert(os.Mkdir(dir, 0o700), qt.IsNil)
				outside := plantOutsideSecret(c, base)
				path := writeAtlasFileConfig(c, dir, tt.plant(c, dir, outside))

				cfg, err := loader.load(c, path)

				c.Assert(err, qt.ErrorMatches, tt.err)
				c.Assert(cfg.DatabaseURL, qt.Equals, "")
				// A refusal that quotes the file it refused to read would be a
				// smaller version of the same leak.
				c.Assert(err.Error(), qt.Not(qt.Contains), outsideFileMarker)
			})
		}
	}
}

// The other half of the guard: everything inside the directory still reads,
// including symbolic links that stay inside it. Without this, "refuse
// everything" would pass the table above.
func TestAtlasFileSandboxReadsInsideTheConfigDirectory(t *testing.T) {
	tests := []struct {
		name  string
		plant func(c *qt.C, dir string) string
	}{
		{
			name:  "plain file",
			plant: insidePlainFile,
		},
		{
			name:  "file in a subdirectory",
			plant: insideSubdirectoryFile,
		},
		{
			name:  "symbolic link to a sibling",
			plant: insideSiblingSymlink,
		},
		{
			name:  "symbolic link in a subdirectory pointing up to a sibling",
			plant: insideUpwardSymlink,
		},
		{
			name:  "explicit ./ prefix",
			plant: insideDotSlashFile,
		},
	}

	for _, loader := range atlasFileLoaders() {
		for _, tt := range tests {
			t.Run(loader.name+"/"+tt.name, func(t *testing.T) {
				c := qt.New(t)

				base := t.TempDir()
				dir := filepath.Join(base, "project")
				c.Assert(os.Mkdir(dir, 0o700), qt.IsNil)
				plantOutsideSecret(c, base)
				path := writeAtlasFileConfig(c, dir, tt.plant(c, dir))

				cfg, err := loader.load(c, path)

				c.Assert(err, qt.IsNil)
				c.Assert(cfg.DatabaseURL, qt.Equals, insideFileMarker)
			})
		}
	}
}

func insidePlainFile(c *qt.C, dir string) string {
	c.Helper()

	c.Assert(os.WriteFile(filepath.Join(dir, "url.txt"), []byte(insideFileMarker), 0o600), qt.IsNil)
	return "url.txt"
}

func insideDotSlashFile(c *qt.C, dir string) string {
	c.Helper()

	c.Assert(os.WriteFile(filepath.Join(dir, "url.txt"), []byte(insideFileMarker), 0o600), qt.IsNil)
	return "./url.txt"
}

func insideSubdirectoryFile(c *qt.C, dir string) string {
	c.Helper()

	c.Assert(os.Mkdir(filepath.Join(dir, "conf"), 0o700), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "conf", "url.txt"), []byte(insideFileMarker), 0o600), qt.IsNil)
	return "conf/url.txt"
}

func insideSiblingSymlink(c *qt.C, dir string) string {
	c.Helper()

	c.Assert(os.WriteFile(filepath.Join(dir, "url.txt"), []byte(insideFileMarker), 0o600), qt.IsNil)
	symlink(c, "url.txt", filepath.Join(dir, "url.link"))
	return "url.link"
}

func insideUpwardSymlink(c *qt.C, dir string) string {
	c.Helper()

	c.Assert(os.WriteFile(filepath.Join(dir, "url.txt"), []byte(insideFileMarker), 0o600), qt.IsNil)
	c.Assert(os.Mkdir(filepath.Join(dir, "conf"), 0o700), qt.IsNil)
	symlink(c, filepath.Join("..", "url.txt"), filepath.Join(dir, "conf", "url.link"))
	return "conf/url.link"
}

// writeAtlasFilesetConfig writes an atlas.hcl whose schema sources are a
// fileset() glob, so the resolved list is readable from the parsed config.
func writeAtlasFilesetConfig(c *qt.C, dir, glob string) string {
	c.Helper()

	path := filepath.Join(dir, "atlas.hcl")
	body := "data \"hcl_schema\" \"app\" {\n  paths = fileset(" + quoteHCL(glob) + ")\n}\n\n" +
		"env \"local\" {\n  src = data.hcl_schema.app.url\n}\n"
	c.Assert(os.WriteFile(path, []byte(body), 0o600), qt.IsNil)
	return path
}

// fileset() walks the same sandbox. An entry that leaves the directory fails
// the whole call rather than being quietly dropped from the list: the returned
// paths become schema-source URLs that another reader opens, and silently
// shortening a list the author wrote is how a schema goes missing without
// anyone being told.
func TestAtlasFilesetSandboxRefusesEscapingEntries(t *testing.T) {
	tests := []struct {
		name  string
		glob  string
		plant func(c *qt.C, dir, outside string)
		err   string
	}{
		{
			name:  "glob matching a link out of the directory",
			glob:  "*.hcl",
			plant: plantEscapingFilesetEntry,
			err:   `.*path escapes atlas\.hcl directory: leaked\.hcl: leaked\.hcl is a symbolic link pointing outside it.*`,
		},
		{
			name:  "recursive glob matching a link out of the directory",
			glob:  "**/*.hcl",
			plant: plantEscapingNestedFilesetEntry,
			err:   `.*path escapes atlas\.hcl directory: nested/leaked\.hcl: nested/leaked\.hcl is a symbolic link pointing outside it.*`,
		},
		{
			name:  "parent traversal glob",
			glob:  "../*.hcl",
			plant: plantSiblingSchema,
			err:   `.*path escapes atlas\.hcl directory: \.\./\*\.hcl: atlas\.hcl file\(\) and fileset\(\) read only inside the directory holding atlas\.hcl.*`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			base := t.TempDir()
			dir := filepath.Join(base, "project")
			c.Assert(os.Mkdir(dir, 0o700), qt.IsNil)
			outside := plantOutsideSchema(c, base)
			tt.plant(c, dir, outside)
			path := writeAtlasFilesetConfig(c, dir, tt.glob)

			cfg, err := projectconfig.LoadAtlasFile(path, "local")

			c.Assert(err, qt.ErrorMatches, tt.err)
			c.Assert(cfg.SchemaSources, qt.HasLen, 0)
		})
	}
}

// The fileset() control: a glob that stays inside the directory still resolves,
// links included.
func TestAtlasFilesetResolvesEntriesInsideTheConfigDirectory(t *testing.T) {
	c := qt.New(t)

	base := t.TempDir()
	dir := filepath.Join(base, "project")
	c.Assert(os.Mkdir(dir, 0o700), qt.IsNil)
	plantOutsideSchema(c, base)
	c.Assert(os.Mkdir(filepath.Join(dir, "schema"), 0o700), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "schema", "a.hcl"), []byte("schema \"main\" {\n}\n"), 0o600), qt.IsNil)
	symlink(c, "a.hcl", filepath.Join(dir, "schema", "b.hcl"))
	path := writeAtlasFilesetConfig(c, dir, "schema/*.hcl")

	cfg, err := projectconfig.LoadAtlasFile(path, "local")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.SchemaSources, qt.DeepEquals, []string{"file://schema/a.hcl", "file://schema/b.hcl"})
}

func plantOutsideSchema(c *qt.C, base string) string {
	c.Helper()

	path := filepath.Join(base, "leaked.hcl")
	c.Assert(os.WriteFile(path, []byte("schema \"main\" {\n}\n"), 0o600), qt.IsNil)
	return path
}

func plantEscapingFilesetEntry(c *qt.C, dir, outside string) {
	c.Helper()

	symlink(c, outside, filepath.Join(dir, "leaked.hcl"))
}

func plantEscapingNestedFilesetEntry(c *qt.C, dir, outside string) {
	c.Helper()

	c.Assert(os.Mkdir(filepath.Join(dir, "nested"), 0o700), qt.IsNil)
	symlink(c, outside, filepath.Join(dir, "nested", "leaked.hcl"))
}

func plantSiblingSchema(c *qt.C, _, _ string) {
	c.Helper()
}
