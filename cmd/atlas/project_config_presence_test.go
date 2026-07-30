package atlas_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/atlas"
)

func TestCompatCommand_ProjectConfigExplicitEmptyMigrationDirFails(t *testing.T) {
	c := qt.New(t)
	t.Chdir(t.TempDir())
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "local" {
  migration {
    dir = ""
  }
}
`), 0o600), qt.IsNil)

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "hash", "--env", "local"})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `atlas\.hcl migration\.dir: migration directories URL is required`)
}

func TestCompatCommand_ProjectConfigExplicitEmptyMigrationFormatFails(t *testing.T) {
	c := qt.New(t)
	t.Chdir(t.TempDir())
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "local" {
  migration {
    format = ""
  }
}
`), 0o600), qt.IsNil)

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "hash", "--env", "local", "--dir", "file://migrations"})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `atlas migrate hash --dir-format: migration directory format must not be empty`)
}

func TestCompatCommand_ProjectConfigEmptyGitBaseKeepsLatestSelector(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	t.Chdir(root)
	c.Assert(os.Mkdir("migrations", 0o750), qt.IsNil)
	c.Assert(
		os.WriteFile(
			filepath.Join("migrations", "1.sql"),
			[]byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"),
			0o600,
		),
		qt.IsNil,
	)
	c.Assert(os.WriteFile("atlas.hcl", []byte(`lint {
  latest = 1
}
env "ci" {
  lint {
    git {
      base = ""
      dir  = ""
    }
  }
}
`), 0o600), qt.IsNil)

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "lint",
		"--env", "ci",
		"--dir", "migrations",
		"--dev-url", "sqlite://" + filepath.Join(root, "dev.db"),
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil, qt.Commentf("command output:\n%s", out.String()))
}

func TestCompatCommand_ExplicitGitBaseSuppressesProjectLatest(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	t.Chdir(root)
	c.Assert(os.Mkdir("migrations", 0o750), qt.IsNil)
	c.Assert(
		os.WriteFile(
			filepath.Join("migrations", "1.sql"),
			[]byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"),
			0o600,
		),
		qt.IsNil,
	)
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "ci" {
  lint {
    latest = 1
  }
}
`), 0o600), qt.IsNil)

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "lint",
		"--env", "ci",
		"--dir", "migrations",
		"--dev-url", "sqlite://" + filepath.Join(root, "dev.db"),
		"--git-base=-unsafe",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `--git-base "-unsafe" is not a safe Git ref`)
	c.Assert(out.String(), qt.Not(qt.Contains), "--latest and --git-base are mutually exclusive")
}

func TestCompatCommand_ExplicitLatestSuppressesProjectGitSelector(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	t.Chdir(root)
	c.Assert(os.Mkdir("migrations", 0o750), qt.IsNil)
	c.Assert(
		os.WriteFile(
			filepath.Join("migrations", "1.sql"),
			[]byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"),
			0o600,
		),
		qt.IsNil,
	)
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "ci" {
  lint {
    git {
      base = "-unsafe"
      dir  = "/not/a/repository"
    }
  }
}
`), 0o600), qt.IsNil)

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "lint",
		"--env", "ci",
		"--dir", "migrations",
		"--dev-url", "sqlite://" + filepath.Join(root, "dev.db"),
		"--latest", "1",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil, qt.Commentf("command output:\n%s", out.String()))
}

func TestCompatCommand_ProjectConfigExplicitEmptySchemaSourcesFail(t *testing.T) {
	c := qt.New(t)
	t.Chdir(t.TempDir())
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "local" {
  src = []
}
`), 0o600), qt.IsNil)

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"schema", "test", "--env", "local"})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `atlas\.hcl schema\.src: desired schema source is required`)
}
