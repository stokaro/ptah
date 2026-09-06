package assistconfig_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/assistconfig"
)

// writeConfig puts a configuration file in a temporary directory and returns
// options that read it.
func writeConfig(c *qt.C, content string, environment map[string]string) assistconfig.Options {
	c.Helper()
	path := filepath.Join(c.TempDir(), assistconfig.FileName)
	c.Assert(os.WriteFile(path, []byte(content), 0o600), qt.IsNil)
	return assistconfig.Options{Path: path, Environ: environ(environment)}
}

// environ builds a lookup over a map, so a test never touches the process
// environment and two tests never race over it.
func environ(values map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		value, set := values[name]
		return value, set
	}
}

// noConfig points at a file that does not exist.
func noConfig(c *qt.C, environment map[string]string) assistconfig.Options {
	c.Helper()
	return assistconfig.Options{
		Path:    filepath.Join(c.TempDir(), "absent.yaml"),
		Environ: environ(environment),
	}
}

const twoProfiles = `
default: work

profiles:
  work:
    type: anthropic
    model: a-model
    credential: env:WORK_KEY
  local:
    type: openai-compatible
    base_url: http://127.0.0.1:11434/v1
    model: qwen
`

func TestLoad_HappyPath(t *testing.T) {
	c := qt.New(t)

	config, err := assistconfig.Load(writeConfig(c, twoProfiles, nil))

	c.Assert(err, qt.IsNil)
	c.Assert(config.Names(), qt.DeepEquals, []string{"local", "work"})
	c.Assert(config.Default, qt.Equals, "work")

	work, err := config.Profile("work")
	c.Assert(err, qt.IsNil)
	c.Assert(work.Type, qt.Equals, assistconfig.TypeAnthropic)
	c.Assert(work.Credential, qt.Equals, "env:WORK_KEY")
	c.Assert(config.Derived("work"), qt.IsFalse)
}

func TestLoad_AMissingFileIsNotAnError(t *testing.T) {
	// An operator who exported a key and nothing else should reach a working
	// provider test without writing configuration first.
	c := qt.New(t)

	config, err := assistconfig.Load(noConfig(c, nil))

	c.Assert(err, qt.IsNil)
	c.Assert(config.Names(), qt.HasLen, 0)
	c.Assert(config.Path, qt.Equals, "")
}

func TestLoad_DerivesProfilesFromTheEnvironment(t *testing.T) {
	tests := []struct {
		name        string
		environment map[string]string
		wantProfile string
		wantType    assistconfig.ProviderType
		wantBase    string
	}{
		{
			name:        "an OpenAI key",
			environment: map[string]string{"OPENAI_API_KEY": "k"},
			wantProfile: "openai",
			wantType:    assistconfig.TypeOpenAICompatible,
			wantBase:    "https://api.openai.com/v1",
		},
		{
			name: "an OpenAI key pointed at a gateway",
			environment: map[string]string{
				"OPENAI_API_KEY":  "k",
				"OPENAI_BASE_URL": "https://gateway.example/v1",
			},
			wantProfile: "openai",
			wantType:    assistconfig.TypeOpenAICompatible,
			wantBase:    "https://gateway.example/v1",
		},
		{
			name:        "an Anthropic key",
			environment: map[string]string{"ANTHROPIC_API_KEY": "k"},
			wantProfile: "anthropic",
			wantType:    assistconfig.TypeAnthropic,
			wantBase:    "",
		},
		{
			name:        "a local model server named by host",
			environment: map[string]string{"OLLAMA_HOST": "127.0.0.1:11434"},
			wantProfile: "ollama",
			wantType:    assistconfig.TypeOpenAICompatible,
			wantBase:    "http://127.0.0.1:11434/v1",
		},
		{
			name:        "a local model server named by URL",
			environment: map[string]string{"OLLAMA_HOST": "http://box.local:11434"},
			wantProfile: "ollama",
			wantType:    assistconfig.TypeOpenAICompatible,
			wantBase:    "http://box.local:11434/v1",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			config, err := assistconfig.Load(noConfig(c, test.environment))

			c.Assert(err, qt.IsNil)
			profile, profileErr := config.Profile(test.wantProfile)
			c.Assert(profileErr, qt.IsNil)
			c.Assert(profile.Type, qt.Equals, test.wantType)
			c.Assert(profile.BaseURL, qt.Equals, test.wantBase)
			c.Assert(config.Derived(test.wantProfile), qt.IsTrue)
			c.Assert(config.Default, qt.Equals, test.wantProfile,
				qt.Commentf("one profile and no stated default is not a guess"))
		})
	}
}

func TestLoad_AWrittenProfileWinsOverADerivedOne(t *testing.T) {
	// An operator who wrote a profile called `openai` meant theirs.
	c := qt.New(t)
	written := `
profiles:
  openai:
    type: openai-compatible
    base_url: https://mine.example/v1
    model: mine
`
	opts := writeConfig(c, written, map[string]string{
		"OPENAI_API_KEY":  "k",
		"OPENAI_BASE_URL": "https://theirs.example/v1",
	})

	config, err := assistconfig.Load(opts)

	c.Assert(err, qt.IsNil)
	profile, err := config.Profile("openai")
	c.Assert(err, qt.IsNil)
	c.Assert(profile.BaseURL, qt.Equals, "https://mine.example/v1")
	c.Assert(config.Derived("openai"), qt.IsFalse)
}

func TestLoad_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		content string
		wantErr string
	}{
		{
			name:    "an unknown provider type",
			content: "profiles:\n  x:\n    type: telepathy\n",
			wantErr: `.*profile "x": unknown type "telepathy": want one of openai-compatible, anthropic`,
		},
		{
			name:    "an openai-compatible profile with no endpoint",
			content: "profiles:\n  x:\n    type: openai-compatible\n    model: m\n",
			wantErr: `(?s).*profile "x": an openai-compatible profile needs a base_url.*`,
		},
		{
			name: "a key written into configuration",
			content: "profiles:\n  x:\n    type: anthropic\n" +
				"    credential: sk-not-a-reference\n",
			wantErr: `(?s).*profile "x": .*is not a reference; write env:NAME or file:PATH.*`,
		},
		{
			name: "an unknown credential kind",
			content: "profiles:\n  x:\n    type: anthropic\n" +
				"    credential: keychain:ptah/work\n",
			wantErr: `.*profile "x": .*unknown reference kind "keychain"; want env or file`,
		},
		{
			// A misspelled key that is silently ignored is a setting the
			// operator believes they made.
			name:    "a misspelled field",
			content: "profiles:\n  x:\n    type: anthropic\n    base_uri: https://example\n",
			wantErr: `(?s)parse .*field base_uri not found.*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			config, err := assistconfig.Load(writeConfig(c, test.content, nil))

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(config, qt.IsNil)
		})
	}
}

func TestSelect_PrefersTheRequestThenTheDefault(t *testing.T) {
	c := qt.New(t)
	config, err := assistconfig.Load(writeConfig(c, twoProfiles, nil))
	c.Assert(err, qt.IsNil)

	requested, err := config.Select("local")
	c.Assert(err, qt.IsNil)
	c.Assert(requested.Name, qt.Equals, "local")

	fallback, err := config.Select("")
	c.Assert(err, qt.IsNil)
	c.Assert(fallback.Name, qt.Equals, "work")
}

func TestSelect_TheEnvironmentPicksTheDefaultWhenTheFileDoesNot(t *testing.T) {
	c := qt.New(t)
	noDefault := `
profiles:
  work:
    type: anthropic
    model: a-model
  local:
    type: openai-compatible
    base_url: http://127.0.0.1:11434/v1
    model: qwen
`
	opts := writeConfig(c, noDefault, map[string]string{"PTAH_ASSIST_PROFILE": "local"})

	config, err := assistconfig.Load(opts)
	c.Assert(err, qt.IsNil)

	selected, err := config.Select("")
	c.Assert(err, qt.IsNil)
	c.Assert(selected.Name, qt.Equals, "local")
}

func TestSelect_FailurePath(t *testing.T) {
	t.Run("two profiles and no default", func(t *testing.T) {
		// Resolving this alphabetically would be Ptah deciding which provider
		// an operator's schema is sent to.
		c := qt.New(t)
		noDefault := `
profiles:
  work:
    type: anthropic
    model: a-model
  local:
    type: openai-compatible
    base_url: http://127.0.0.1:11434/v1
    model: qwen
`
		config, err := assistconfig.Load(writeConfig(c, noDefault, nil))
		c.Assert(err, qt.IsNil)

		_, err = config.Select("")

		c.Assert(err, qt.ErrorIs, assistconfig.ErrNoProfile)
		c.Assert(err, qt.ErrorMatches, ".*configured profiles are local, work")
	})

	t.Run("a name no profile carries", func(t *testing.T) {
		c := qt.New(t)
		config, err := assistconfig.Load(writeConfig(c, twoProfiles, nil))
		c.Assert(err, qt.IsNil)

		_, err = config.Select("staging")

		c.Assert(err, qt.ErrorIs, assistconfig.ErrUnknownProfile)
		c.Assert(err, qt.ErrorMatches, `.*"staging".*local, work`)
	})
}

func TestProvider_BuildsTheAdapterTheProfileNames(t *testing.T) {
	tests := []struct {
		name        string
		content     string
		profile     string
		wantModel   string
		environment map[string]string
	}{
		{
			name:        "anthropic",
			content:     twoProfiles,
			profile:     "work",
			wantModel:   "a-model",
			environment: map[string]string{"WORK_KEY": "not-a-real-key"},
		},
		{
			name:      "openai-compatible",
			content:   twoProfiles,
			profile:   "local",
			wantModel: "qwen",
		},
		{
			name:        "a model from the environment",
			content:     "profiles:\n  work:\n    type: anthropic\n",
			profile:     "work",
			wantModel:   "from-the-environment",
			environment: map[string]string{"PTAH_ASSIST_MODEL": "from-the-environment"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			opts := writeConfig(c, test.content, test.environment)
			config, err := assistconfig.Load(opts)
			c.Assert(err, qt.IsNil)
			profile, err := config.Profile(test.profile)
			c.Assert(err, qt.IsNil)

			provider, err := config.Provider(profile, opts)

			c.Assert(err, qt.IsNil)
			c.Assert(provider.Profile(), qt.Equals, test.profile)
			c.Assert(provider.Model(), qt.Equals, test.wantModel)
		})
	}
}

func TestProvider_FailurePath(t *testing.T) {
	tests := []struct {
		name        string
		content     string
		profile     string
		environment map[string]string
		wantErr     string
	}{
		{
			name:    "no model anywhere",
			content: "profiles:\n  work:\n    type: anthropic\n",
			profile: "work",
			wantErr: `profile "work" states no model: set it in the profile, ` +
				`export PTAH_ASSIST_MODEL, or pass --model`,
		},
		{
			name:    "a credential variable that is not set",
			content: twoProfiles,
			profile: "work",
			wantErr: `profile "work": .*WORK_KEY is not set in this environment`,
		},
		{
			name:        "a credential variable exported empty",
			content:     twoProfiles,
			profile:     "work",
			environment: map[string]string{"WORK_KEY": ""},
			wantErr:     `profile "work": .*WORK_KEY is set to an empty value`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			opts := writeConfig(c, test.content, test.environment)
			config, err := assistconfig.Load(opts)
			c.Assert(err, qt.IsNil)
			profile, err := config.Profile(test.profile)
			c.Assert(err, qt.IsNil)

			provider, err := config.Provider(profile, opts)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(provider, qt.IsNil)
		})
	}
}

func TestProvider_ReadsACredentialFromAFile(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	keyPath := filepath.Join(dir, "key")
	// The trailing newline every editor adds: sent as part of the key it
	// becomes an invalid-credential error that points at the wrong thing.
	c.Assert(os.WriteFile(keyPath, []byte("not-a-real-key\n"), 0o600), qt.IsNil)

	content := "profiles:\n  work:\n    type: anthropic\n    model: m\n    credential: file:" + keyPath + "\n"
	opts := writeConfig(c, content, nil)
	config, err := assistconfig.Load(opts)
	c.Assert(err, qt.IsNil)
	profile, err := config.Profile("work")
	c.Assert(err, qt.IsNil)

	provider, err := config.Provider(profile, opts)

	c.Assert(err, qt.IsNil)
	c.Assert(provider, qt.IsNotNil)
}

func TestDefaultPath_IsTheSameDotPtahThisTreeUsesBesideAProject(t *testing.T) {
	// One directory name in both positions: `./.ptah` beside a project already
	// holds the approval keys and the agent audit record, and a home-directory
	// location spelled `.config/ptah` would make "where does Ptah keep things"
	// two answers.
	c := qt.New(t)

	path, err := assistconfig.DefaultPath(assistconfig.Options{
		Environ: environ(nil),
		HomeDir: func() (string, error) { return "/somewhere/home", nil },
	})

	c.Assert(err, qt.IsNil)
	c.Assert(path, qt.Equals, filepath.Join("/somewhere/home", ".ptah", "assist.yaml"))
}

func TestDefaultPath_AnExplicitPathWins(t *testing.T) {
	c := qt.New(t)

	path, err := assistconfig.DefaultPath(assistconfig.Options{
		Environ: environ(map[string]string{"PTAH_ASSIST_CONFIG": "/somewhere/else.yaml"}),
		HomeDir: func() (string, error) { return "/somewhere/home", nil },
	})

	c.Assert(err, qt.IsNil)
	c.Assert(path, qt.Equals, "/somewhere/else.yaml")
}

func TestDefaultPath_ReportsAHomeDirectoryItCannotResolve(t *testing.T) {
	// A container with no HOME is a real environment, and answering with a
	// relative path there would write profiles into the working directory.
	c := qt.New(t)

	path, err := assistconfig.DefaultPath(assistconfig.Options{
		Environ: environ(nil),
		HomeDir: func() (string, error) { return "", errors.New("$HOME is not defined") },
	})

	c.Assert(err, qt.ErrorMatches, `resolve the home directory: \$HOME is not defined`)
	c.Assert(path, qt.Equals, "")
}
