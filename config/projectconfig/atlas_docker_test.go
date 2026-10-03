package projectconfig_test

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/config/projectconfig"
	"ptah.run/internal/devdocker"
)

// A top-level docker block declares a dev database, and a reference to its url
// names it (stokaro/ptah#4041). The tests below read the URL the reference
// evaluates to, and provision it through devdocker with a runtime that starts
// nothing, so what the block declared is read back from what the provisioner
// was asked to do.

// parseDockerProject parses a project whose env "dev" names the dev database
// with devExpr, beside blocks.
func parseDockerProject(c *qt.C, dir, blocks, devExpr string, opts projectconfig.AtlasLoadOptions) (projectconfig.Config, error) {
	c.Helper()
	opts.EnvName = "dev"
	return projectconfig.ParseAtlasWithOptions([]byte(blocks+`
env "dev" {
  url = "postgres://localhost/app"
  dev = `+devExpr+`
}
`), filepath.Join(dir, "atlas.hcl"), opts)
}

func TestParseAtlas_DockerBlockURL_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		blocks  string
		devExpr string
		// want matches the URL up to the declaration's name, which carries a
		// digest of its content.
		want string
	}{
		{
			name:    "postgres with a schema",
			blocks:  "docker \"postgres\" \"dev\" {\n  image  = \"postgres:17\"\n  schema = \"public\"\n}\n",
			devExpr: "docker.postgres.dev.url",
			want:    `docker\+postgres://_/postgres:17/postgres\?search_path=public#docker\.postgres\.dev\.[0-9a-f]{12}`,
		},
		{
			name:    "postgres with a database",
			blocks:  "docker \"postgres\" \"dev\" {\n  image    = \"postgres:17\"\n  database = \"app\"\n}\n",
			devExpr: "docker.postgres.dev.url",
			want:    `docker\+postgres://_/postgres:17/app#docker\.postgres\.dev\.[0-9a-f]{12}`,
		},
		{
			// The tag keeps the URL's last segment from reading as a database.
			name:    "an image with no tag",
			blocks:  "docker \"postgres\" \"dev\" {\n  image = \"acme/pg\"\n}\n",
			devExpr: "docker.postgres.dev.url",
			want:    `docker\+postgres://_/acme/pg:latest/postgres#docker\.postgres\.dev\.[0-9a-f]{12}`,
		},
		{
			name:    "mysql with a schema",
			blocks:  "docker \"mysql\" \"dev\" {\n  image  = \"mysql:8.4\"\n  schema = \"app\"\n}\n",
			devExpr: "docker.mysql.dev.url",
			want:    `docker\+mysql://_/mysql:8.4/app#docker\.mysql\.dev\.[0-9a-f]{12}`,
		},
		{
			name:    "mysql with no schema is the whole server",
			blocks:  "docker \"mysql\" \"dev\" {\n  image = \"mysql:8.4\"\n}\n",
			devExpr: "docker.mysql.dev.url",
			want:    `docker\+mysql://_/mysql:8.4#docker\.mysql\.dev\.[0-9a-f]{12}`,
		},
		{
			name:    "mariadb",
			blocks:  "docker \"mariadb\" \"dev\" {\n  image  = \"mariadb:11.8\"\n  schema = \"app\"\n}\n",
			devExpr: "docker.mariadb.dev.url",
			want:    `docker\+mariadb://_/mariadb:11.8/app#docker\.mariadb\.dev\.[0-9a-f]{12}`,
		},
		{
			// The block is read once the evaluation context exists, so it may
			// use a local, and a local may name it.
			name: "through locals",
			blocks: "locals {\n  image = \"postgres:17\"\n  dev   = docker.postgres.dev.url\n}\n" +
				"docker \"postgres\" \"dev\" {\n  image = local.image\n}\n",
			devExpr: "local.dev",
			want:    `docker\+postgres://_/postgres:17/postgres#docker\.postgres\.dev\.[0-9a-f]{12}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			cfg, err := parseDockerProject(c, c.TempDir(), test.blocks, test.devExpr, projectconfig.AtlasLoadOptions{})
			c.Assert(err, qt.IsNil)
			c.Assert(cfg.DevURL, qt.Matches, test.want)
			c.Assert(cfg.IgnoredConstructs, qt.HasLen, 0)
		})
	}
}

// dockerRecorder is a devdocker runtime that starts nothing and records what
// it was asked to do, with the steps after readiness answered by the recorder.
type dockerRecorder struct {
	mu        sync.Mutex
	builds    []devdocker.Build
	images    []string
	env       [][]string
	baselines []string
}

func (r *dockerRecorder) Available(context.Context) error { return nil }

func (r *dockerRecorder) Start(_ context.Context, _, image, _ string, env []string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.images = append(r.images, image)
	r.env = append(r.env, env)
	return "127.0.0.1:15432", nil
}

func (r *dockerRecorder) Remove(context.Context, string) error { return nil }

func (r *dockerRecorder) Stopped(context.Context, string) (string, error) { return "", nil }

func (r *dockerRecorder) Build(_ context.Context, _ string, build devdocker.Build) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.builds = append(r.builds, build)
	return nil
}

func (r *dockerRecorder) RemoveImage(context.Context, string) error { return nil }

func (r *dockerRecorder) baseline(_ context.Context, _, _, baseline string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.baselines = append(r.baselines, baseline)
	return nil
}

func (r *dockerRecorder) options() devdocker.Options {
	return devdocker.Options{
		Runner:         r,
		Ready:          func(context.Context, string) error { return nil },
		CreateDatabase: func(context.Context, string, string, string) error { return nil },
		RunBaseline:    r.baseline,
	}
}

// TestParseAtlas_DockerBlockDeclaresWhatTheURLCannotCarry provisions the URL a
// block's reference evaluates to, and reads back the build, the baseline and
// the environment the block declared. The build context resolves against the
// atlas.hcl directory.
func TestParseAtlas_DockerBlockDeclaresWhatTheURLCannotCarry(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	cfg, err := parseDockerProject(c, dir, `docker "postgres" "dev" {
  image    = "ptah-dev:custom"
  schema   = "public"
  env      = ["TZ=UTC"]
  baseline = <<-SQL
    CREATE SCHEMA audit;
  SQL
  build {
    context    = "db"
    dockerfile = "Dockerfile.dev"
    target     = "dev"
    platform   = "linux/amd64"
    args = {
      PG = "17"
    }
  }
}
`, "docker.postgres.dev.url", projectconfig.AtlasLoadOptions{})
	c.Assert(err, qt.IsNil)
	recorder := &dockerRecorder{}

	_, release, err := devdocker.Resolve(t.Context(), cfg.DevURL, recorder.options())
	c.Assert(err, qt.IsNil)
	release()

	c.Assert(recorder.builds, qt.DeepEquals, []devdocker.Build{{
		Context:    filepath.Join(dir, "db"),
		Dockerfile: "Dockerfile.dev",
		Target:     "dev",
		Platform:   "linux/amd64",
		Args:       map[string]string{"PG": "17"},
	}})
	c.Assert(recorder.images, qt.DeepEquals, []string{"ptah-dev:custom"})
	c.Assert(recorder.baselines, qt.DeepEquals, []string{"CREATE SCHEMA audit;\n"})
	c.Assert(recorder.env, qt.HasLen, 1)
	c.Assert(recorder.env[0][0], qt.Equals, "TZ=UTC")
}

// TestParseAtlas_DockerBlockTimeoutBoundsTheReadinessWait reads the timeout
// back from a wait that never succeeds.
func TestParseAtlas_DockerBlockTimeoutBoundsTheReadinessWait(t *testing.T) {
	c := qt.New(t)
	cfg, err := parseDockerProject(c, c.TempDir(),
		"docker \"postgres\" \"dev\" {\n  image   = \"postgres:17\"\n  timeout = \"1ms\"\n}\n",
		"docker.postgres.dev.url", projectconfig.AtlasLoadOptions{})
	c.Assert(err, qt.IsNil)
	opts := (&dockerRecorder{}).options()
	opts.Ready = func(context.Context, string) error { return errors.New("connection refused") }

	_, release, err := devdocker.Resolve(t.Context(), cfg.DevURL, opts)
	t.Cleanup(release)

	c.Assert(err, qt.ErrorMatches, `dev database postgres:17 did not become ready: timed out after 1ms: connection refused`)
}

func TestParseAtlas_DockerBlockURL_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		blocks  string
		devExpr string
		wantErr string
	}{
		{
			name:    "an engine Ptah starts no dev database of",
			blocks:  "docker \"sqlserver\" \"dev\" {\n  image = \"mcr.microsoft.com/mssql/server:2022-latest\"\n}\n",
			devExpr: "docker.sqlserver.dev.url",
			wantErr: `unsupported atlas.hcl construct "docker.sqlserver" at .*atlas.hcl:1`,
		},
		{
			name:    "an attribute the reference lists and Ptah does not read",
			blocks:  "docker \"postgres\" \"dev\" {\n  image   = \"postgres:17\"\n  volumes = [\"/data:/data\"]\n}\n",
			devExpr: "docker.postgres.dev.url",
			wantErr: `unsupported atlas.hcl construct "docker.postgres.dev.volumes" at .*atlas.hcl:3`,
		},
		{
			name:    "a database on mysql, whose schema is the database",
			blocks:  "docker \"mysql\" \"dev\" {\n  image    = \"mysql:8.4\"\n  database = \"app\"\n}\n",
			devExpr: "docker.mysql.dev.url",
			wantErr: `unsupported atlas.hcl construct "docker.mysql.dev.database" at .*atlas.hcl:3`,
		},
		{
			name:    "a connection block",
			blocks:  "docker \"postgres\" \"dev\" {\n  image = \"postgres:17\"\n  connection {\n    max_open = 1\n  }\n}\n",
			devExpr: "docker.postgres.dev.url",
			wantErr: `unsupported atlas.hcl construct "docker.postgres.dev.connection" at .*atlas.hcl:3`,
		},
		{
			name:    "two builds",
			blocks:  "docker \"postgres\" \"dev\" {\n  image = \"postgres:17\"\n  build {\n    context = \".\"\n  }\n  build {\n    context = \".\"\n  }\n}\n",
			devExpr: "docker.postgres.dev.url",
			wantErr: `unsupported atlas.hcl construct "docker.postgres.dev.build" at .*atlas.hcl:6`,
		},
		{
			name:    "a build attribute Ptah does not read",
			blocks:  "docker \"postgres\" \"dev\" {\n  image = \"postgres:17\"\n  build {\n    context = \".\"\n    secrets = [\"x\"]\n  }\n}\n",
			devExpr: "docker.postgres.dev.url",
			wantErr: `unsupported atlas.hcl construct "docker.postgres.dev.build.secrets" at .*atlas.hcl:5`,
		},
		{
			name:    "no image",
			blocks:  "docker \"postgres\" \"dev\" {\n  schema = \"public\"\n}\n",
			devExpr: "docker.postgres.dev.url",
			wantErr: `atlas.hcl docker.postgres.dev at .*atlas.hcl:1 requires image`,
		},
		{
			name:    "an empty image",
			blocks:  "docker \"postgres\" \"dev\" {\n  image = \"\"\n}\n",
			devExpr: "docker.postgres.dev.url",
			wantErr: `atlas.hcl "image" at .*atlas.hcl:2 must not be empty`,
		},
		{
			name:    "a build with no context",
			blocks:  "docker \"postgres\" \"dev\" {\n  image = \"postgres:17\"\n  build {\n    dockerfile = \"Dockerfile\"\n  }\n}\n",
			devExpr: "docker.postgres.dev.url",
			wantErr: `atlas.hcl docker.postgres.dev.build at .*atlas.hcl:3 requires context`,
		},
		{
			name:    "a timeout that is not a duration",
			blocks:  "docker \"postgres\" \"dev\" {\n  image   = \"postgres:17\"\n  timeout = \"soon\"\n}\n",
			devExpr: "docker.postgres.dev.url",
			wantErr: `atlas.hcl docker.postgres.dev timeout "soon" at .*atlas.hcl:3 is not a positive duration`,
		},
		{
			name:    "build arguments that are not a map",
			blocks:  "docker \"postgres\" \"dev\" {\n  image = \"postgres:17\"\n  build {\n    context = \".\"\n    args    = [\"PG=17\"]\n  }\n}\n",
			devExpr: "docker.postgres.dev.url",
			wantErr: `atlas.hcl "args" at .*atlas.hcl:5 must be a map of strings`,
		},
		{
			name:    "the same block twice",
			blocks:  "docker \"postgres\" \"dev\" {\n  image = \"postgres:17\"\n}\ndocker \"postgres\" \"dev\" {\n  image = \"postgres:18\"\n}\n",
			devExpr: "docker.postgres.dev.url",
			wantErr: `duplicate atlas.hcl docker "postgres" "dev" at .*atlas.hcl:4`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			cfg, err := parseDockerProject(c, c.TempDir(), test.blocks, test.devExpr, projectconfig.AtlasLoadOptions{})
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(cfg.DevURL, qt.Equals, "")
		})
	}
}

// TestParseAtlas_UnreferencedDockerBlockStaysIgnored is the laziness control:
// a block no selected env references is not read, so an attribute Ptah would
// refuse there passes, and the block is reported as ignored, as the community
// binary ignores every docker block.
func TestParseAtlas_UnreferencedDockerBlockStaysIgnored(t *testing.T) {
	c := qt.New(t)
	cfg, err := parseDockerProject(c, c.TempDir(),
		"docker \"postgres\" \"dev\" {\n  image   = \"postgres:17\"\n  volumes = [\"/data:/data\"]\n}\n",
		`"postgres://localhost/dev"`, projectconfig.AtlasLoadOptions{})
	c.Assert(err, qt.IsNil)
	c.Assert(cfg.DevURL, qt.Equals, "postgres://localhost/dev")
	c.Assert(cfg.IgnoredConstructs, qt.HasLen, 1)
	c.Assert(cfg.IgnoredConstructs[0].Kind, qt.Equals, "block")
	c.Assert(cfg.IgnoredConstructs[0].Name, qt.Equals, "docker")
}

// TestParseAtlas_IgnoreDockerBlocksAnswersAsCommunityEdition pins the strict
// answer. Measured on the pinned community binary v1.3.0, a reference to a
// docker block's url exits 1 with `Unsupported attribute; This object does not
// have an attribute named "url".`
func TestParseAtlas_IgnoreDockerBlocksAnswersAsCommunityEdition(t *testing.T) {
	c := qt.New(t)
	cfg, err := parseDockerProject(c, c.TempDir(),
		"docker \"postgres\" \"dev\" {\n  image = \"postgres:17\"\n}\n",
		"docker.postgres.dev.url", projectconfig.AtlasLoadOptions{IgnoreDockerBlocks: true})
	c.Assert(err, qt.ErrorMatches, `(?s).*Unsupported attribute; This object does not have an attribute named "url".*`)
	c.Assert(cfg.DevURL, qt.Equals, "")
}
