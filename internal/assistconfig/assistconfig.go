// Package assistconfig resolves which model Ptah Assist talks to, and how it
// gets the credential to do it.
//
// # Provider profiles are the operator's, never the project's
//
// A profile names an endpoint, a model, and a credential reference. Profiles
// are read from the operator's own configuration and from the invocation --
// never from a file inside the repository being worked on.
//
// That boundary is the same one internal/agentpolicy draws, for the same
// reason. A repository that could define a profile could point Ptah Assist at
// an endpoint of its author's choosing, with the operator's key attached, and
// the first thing sent there would be the schema Assist was asked about. A
// project file may SELECT among the profiles the operator defined; selecting
// one that does not exist is an error rather than a definition.
//
// # Ptah stores no key
//
// A profile carries a credential REFERENCE -- `env:NAME` or `file:PATH` -- and
// the value is read at the moment a request is made. This is the same posture
// the rest of the tree already takes: internal/cloudtoken mints, the OCI client
// delegates to the Docker credential store, and no Ptah command has ever
// written a credential to a file it owns.
//
// A credential COMMAND is deliberately absent. It is a real convention
// elsewhere, and it is arbitrary code execution driven by a configuration file,
// on a surface whose whole design refuses the model a way to execute anything.
// Adding it needs its own decision, not an entry in a list of reference kinds.
package assistconfig

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"

	"go.5x5.cz/ptah/internal/aiprovider"
)

// ConfigEnvVar names the file to read instead of the default location.
const ConfigEnvVar = "PTAH_ASSIST_CONFIG"

// ProfileEnvVar names the profile to use when nothing else selects one.
const ProfileEnvVar = "PTAH_ASSIST_PROFILE"

// ModelEnvVar names the model for a profile that does not state one, which is
// how the zero-configuration providers below get a model identifier.
const ModelEnvVar = "PTAH_ASSIST_MODEL"

// FileName is the configuration file's name inside Ptah's configuration
// directory.
const FileName = "assist.yaml"

var (
	// ErrNoProfile reports that no profile was selected and none could be
	// derived.
	ErrNoProfile = errors.New("no Ptah Assist provider profile is configured")
	// ErrUnknownProfile reports a name that no profile carries.
	ErrUnknownProfile = errors.New("unknown provider profile")
	// ErrCredential reports a credential reference that could not be resolved.
	ErrCredential = errors.New("provider credential could not be resolved")
	// ErrInsecureFile reports a file other users can read.
	ErrInsecureFile = errors.New("file is readable by other users")
)

// ProviderType names an adapter.
type ProviderType string

const (
	// TypeOpenAICompatible is every endpoint speaking OpenAI's Chat Completions
	// API: OpenAI, Azure OpenAI, OpenRouter, LiteLLM and other gateways, vLLM,
	// LM Studio, Ollama, MLX.
	TypeOpenAICompatible ProviderType = "openai-compatible"
	// TypeAnthropic is the Messages API.
	TypeAnthropic ProviderType = "anthropic"
)

// ProviderTypes lists the adapters this build has.
func ProviderTypes() []ProviderType {
	return []ProviderType{TypeOpenAICompatible, TypeAnthropic}
}

// Profile is one configured way to reach a model.
type Profile struct {
	// Name is the profile's key in the file; it is filled in on load.
	Name string `yaml:"-"`
	// Type selects the adapter.
	Type ProviderType `yaml:"type"`
	// BaseURL is the endpoint root. Optional for anthropic, required for an
	// OpenAI-compatible endpoint, because there is no default that is right for
	// both a hosted API and a model running on this machine.
	BaseURL string `yaml:"base_url"`
	// Model is the model identifier to send.
	Model string `yaml:"model"`
	// Credential is a reference, never a key: `env:NAME` or `file:PATH`.
	Credential string `yaml:"credential"`
	// Headers are extra headers, for a gateway that wants one.
	Headers map[string]string `yaml:"headers"`
	// Query are extra query parameters, which is how Azure OpenAI carries its
	// api-version.
	Query map[string]string `yaml:"query"`
	// TimeoutSeconds bounds one request. Zero takes the package default, which
	// is generous because a local model's first request includes loading it.
	TimeoutSeconds int `yaml:"timeout_seconds"`
	// MaxRetries bounds retries of a retryable failure. A negative value
	// disables retrying; zero takes the default.
	MaxRetries *int `yaml:"max_retries"`
}

// File is the configuration file's shape.
type File struct {
	// Default names the profile to use when nothing else selects one.
	Default string `yaml:"default"`
	// Profiles are the configured endpoints, keyed by name.
	Profiles map[string]Profile `yaml:"profiles"`
}

// Config is the resolved configuration.
type Config struct {
	// Path is the file the profiles were read from, empty when none was found.
	Path     string
	Default  string
	profiles map[string]Profile
	// derived names the profiles that came from the environment rather than
	// from a file, so a report can say where a profile came from.
	derived map[string]bool
}

// Options configures loading, so a test does not have to write to the
// operator's home directory.
type Options struct {
	// Path overrides the file location.
	Path string
	// Environ reads an environment variable. A nil value reads the process's.
	Environ func(string) (string, bool)
	// ConfigDir returns the directory Ptah's configuration lives in. A nil
	// value resolves the platform's.
	ConfigDir func() (string, error)
}

// lookup resolves an environment variable through the options.
func (o Options) lookup(name string) (string, bool) {
	if o.Environ == nil {
		return os.LookupEnv(name)
	}
	return o.Environ(name)
}

// DefaultPath is where the profiles live when nothing overrides it.
//
// `$XDG_CONFIG_HOME/ptah/assist.yaml` when that variable is set, and the
// platform's own configuration directory otherwise. XDG is honored first
// because a developer who set it means it, including on macOS where Go's own
// answer is Library/Application Support.
func DefaultPath(opts Options) (string, error) {
	if explicit, set := opts.lookup(ConfigEnvVar); set && explicit != "" {
		return explicit, nil
	}
	if xdg, set := opts.lookup("XDG_CONFIG_HOME"); set && xdg != "" {
		return filepath.Join(xdg, "ptah", FileName), nil
	}
	configDir := opts.ConfigDir
	if configDir == nil {
		configDir = os.UserConfigDir
	}
	dir, err := configDir()
	if err != nil {
		return "", fmt.Errorf("resolve the configuration directory: %w", err)
	}
	return filepath.Join(dir, "ptah", FileName), nil
}

// Load reads the operator's profiles, and derives what the environment already
// describes.
//
// A missing file is not an error. An operator who exported a provider key and
// nothing else should reach a working `ptah assist provider test` without
// writing configuration first, which is what every tool in this space does.
func Load(opts Options) (*Config, error) {
	path := opts.Path
	if path == "" {
		resolved, err := DefaultPath(opts)
		if err != nil {
			return nil, err
		}
		path = resolved
	}

	config := &Config{
		profiles: make(map[string]Profile),
		derived:  make(map[string]bool),
	}
	file, found, err := readFile(path)
	if err != nil {
		return nil, err
	}
	if found {
		config.Path = path
		config.Default = file.Default
		for name, profile := range file.Profiles {
			profile.Name = name
			if err := profile.validate(); err != nil {
				return nil, fmt.Errorf("%s: profile %q: %w", path, name, err)
			}
			config.profiles[name] = profile
		}
	}

	for _, derived := range derivedProfiles(opts) {
		if _, defined := config.profiles[derived.Name]; defined {
			continue
		}
		config.profiles[derived.Name] = derived
		config.derived[derived.Name] = true
	}
	if config.Default == "" {
		config.Default = defaultName(opts, config)
	}
	return config, nil
}

// readFile reads and strictly decodes the configuration.
//
// Unknown fields are refused, for the reason config/projectconfig refuses them:
// a misspelled key that is silently ignored is a setting the operator believes
// they made.
func readFile(path string) (File, bool, error) {
	raw, err := os.ReadFile(path) // #nosec G304 -- the path is the operator's own configuration location
	if errors.Is(err, os.ErrNotExist) {
		return File{}, false, nil
	}
	if err != nil {
		return File{}, false, fmt.Errorf("read %s: %w", path, err)
	}
	if err := refuseWorldReadable(path); err != nil {
		return File{}, false, err
	}

	decoder := yaml.NewDecoder(strings.NewReader(string(raw)))
	decoder.KnownFields(true)
	var file File
	// An empty file decodes to io.EOF. An operator who created the file and has
	// not filled it in yet has a valid configuration with no profiles in it,
	// which is different from a broken one.
	if err := decoder.Decode(&file); err != nil && !errors.Is(err, io.EOF) {
		return File{}, false, fmt.Errorf("parse %s: %w", path, err)
	}
	return file, true, nil
}

// defaultName picks the profile to use when the file named none.
//
// The environment wins over a guess, and a single profile is not a guess. Two
// profiles with no default is a question only the operator can answer, so it
// stays unanswered rather than being resolved alphabetically.
func defaultName(opts Options, config *Config) string {
	if selected, set := opts.lookup(ProfileEnvVar); set && selected != "" {
		return selected
	}
	if len(config.profiles) == 1 {
		for name := range config.profiles {
			return name
		}
	}
	return ""
}

// Names lists the configured profiles in a stable order.
func (c *Config) Names() []string {
	names := make([]string, 0, len(c.profiles))
	for name := range c.profiles {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// Profile returns one profile by name.
func (c *Config) Profile(name string) (Profile, error) {
	profile, found := c.profiles[name]
	if !found {
		return Profile{}, fmt.Errorf("%w %q: configured profiles are %s",
			ErrUnknownProfile, name, describeNames(c.Names()))
	}
	return profile, nil
}

// Derived reports whether a profile came from the environment rather than from
// the file, so a listing can say so instead of implying a file nobody wrote.
func (c *Config) Derived(name string) bool {
	return c.derived[name]
}

// describeNames renders a profile list for a diagnostic.
func describeNames(names []string) string {
	if len(names) == 0 {
		return "none"
	}
	return strings.Join(names, ", ")
}

// Select resolves the profile a caller asked for, falling back to the default.
//
// The order is the operator's own: an explicit request, then the environment's
// selection or the file's default. There is no "first profile in the file",
// because a file's order is not a decision.
func (c *Config) Select(requested string) (Profile, error) {
	if requested != "" {
		return c.Profile(requested)
	}
	if c.Default == "" {
		return Profile{}, fmt.Errorf("%w: configured profiles are %s",
			ErrNoProfile, describeNames(c.Names()))
	}
	return c.Profile(c.Default)
}

// Provider builds the adapter a profile describes, resolving its credential
// now.
//
// Resolution happens here rather than at load, so a session that never talks to
// a model never reads the key -- and `ptah assist provider list` can report
// every profile without touching any of their credentials.
func (c *Config) Provider(profile Profile, opts Options) (aiprovider.Provider, error) {
	if profile.Model == "" {
		model, set := opts.lookup(ModelEnvVar)
		if !set || model == "" {
			return nil, fmt.Errorf(
				"profile %q states no model: set it in the profile, export %s, or pass --model",
				profile.Name, ModelEnvVar)
		}
		profile.Model = model
	}
	key, err := resolveCredential(profile.Credential, opts)
	if err != nil {
		return nil, fmt.Errorf("profile %q: %w", profile.Name, err)
	}

	cfg := aiprovider.Config{
		Profile: profile.Name,
		BaseURL: profile.BaseURL,
		Model:   profile.Model,
		APIKey:  key,
		Headers: profile.Headers,
		Query:   profile.Query,
	}
	if profile.TimeoutSeconds > 0 {
		cfg.Timeout = time.Duration(profile.TimeoutSeconds) * time.Second
	}
	if profile.MaxRetries != nil {
		cfg.MaxRetries = *profile.MaxRetries
	} else {
		cfg.MaxRetries = aiprovider.DefaultMaxRetries
	}

	switch profile.Type {
	case TypeAnthropic:
		return aiprovider.NewAnthropic(cfg), nil
	case TypeOpenAICompatible:
		return aiprovider.NewOpenAICompatible(cfg), nil
	}
	return nil, fmt.Errorf("profile %q: unknown provider type %q", profile.Name, profile.Type)
}

// validate refuses a profile that cannot be used, at load rather than at first
// request.
func (p Profile) validate() error {
	if !slices.Contains(ProviderTypes(), p.Type) {
		return fmt.Errorf("unknown type %q: want one of %s", p.Type, describeTypes())
	}
	if p.Type == TypeOpenAICompatible && p.BaseURL == "" {
		return errors.New(
			"an openai-compatible profile needs a base_url: there is no default that suits " +
				"both a hosted API and a model on this machine")
	}
	if p.Credential != "" {
		if _, err := parseCredentialRef(p.Credential); err != nil {
			return err
		}
	}
	return nil
}

// describeTypes renders the adapter list for a diagnostic.
func describeTypes() string {
	names := make([]string, 0, len(ProviderTypes()))
	for _, providerType := range ProviderTypes() {
		names = append(names, string(providerType))
	}
	return strings.Join(names, ", ")
}
