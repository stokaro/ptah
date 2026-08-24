package assistconfig

import "strings"

// Environment variables the whole ecosystem already uses.
//
// They are read rather than invented because an operator arriving with a key
// already exported is the common case, and asking them to write a
// configuration file to reach `ptah assist provider test` would be asking them
// to configure something before they can find out whether it works.
const (
	// OpenAIKeyEnvVar is the conventional name for an OpenAI-compatible key.
	OpenAIKeyEnvVar = "OPENAI_API_KEY"
	// OpenAIBaseEnvVar redirects the OpenAI-compatible profile, which is how
	// every gateway, proxy and local server in this space is reached.
	OpenAIBaseEnvVar = "OPENAI_BASE_URL"
	// AnthropicKeyEnvVar is the conventional name for a Messages API key.
	AnthropicKeyEnvVar = "ANTHROPIC_API_KEY"
	// AnthropicBaseEnvVar redirects the Messages API profile at a gateway.
	AnthropicBaseEnvVar = "ANTHROPIC_BASE_URL"
	// OllamaHostEnvVar is where a local Ollama listens. It is a host rather
	// than a URL by that project's own convention.
	OllamaHostEnvVar = "OLLAMA_HOST"

	// defaultOpenAIBaseURL is the hosted endpoint, used only when a key is
	// exported and no base URL is.
	defaultOpenAIBaseURL = "https://api.openai.com/v1"
)

// derivedProfiles builds the profiles the environment already describes.
//
// Each is named for what it is, so `ptah assist provider list` distinguishes a
// profile the operator wrote from one Ptah inferred, and so a file that defines
// the same name wins -- an operator who wrote a profile called `openai` meant
// theirs.
func derivedProfiles(opts Options) []Profile {
	profiles := make([]Profile, 0, 3)
	if _, set := opts.lookup(OpenAIKeyEnvVar); set {
		profiles = append(profiles, Profile{
			Name:       "openai",
			Type:       TypeOpenAICompatible,
			BaseURL:    envOr(opts, OpenAIBaseEnvVar, defaultOpenAIBaseURL),
			Credential: "env:" + OpenAIKeyEnvVar,
		})
	}
	if _, set := opts.lookup(AnthropicKeyEnvVar); set {
		profiles = append(profiles, Profile{
			Name:       "anthropic",
			Type:       TypeAnthropic,
			BaseURL:    envOr(opts, AnthropicBaseEnvVar, ""),
			Credential: "env:" + AnthropicKeyEnvVar,
		})
	}
	if host, set := opts.lookup(OllamaHostEnvVar); set && host != "" {
		profiles = append(profiles, Profile{
			Name:    "ollama",
			Type:    TypeOpenAICompatible,
			BaseURL: ollamaBaseURL(host),
		})
	}
	return profiles
}

// envOr reads a variable or answers a default.
func envOr(opts Options, name, fallback string) string {
	if value, set := opts.lookup(name); set && value != "" {
		return value
	}
	return fallback
}

// ollamaBaseURL turns that project's host convention into a URL.
//
// It accepts what people actually export: a bare `host:port`, a host with a
// scheme, and a URL that already ends in the version segment.
func ollamaBaseURL(host string) string {
	url := host
	if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
		url = "http://" + url
	}
	if strings.HasSuffix(url, "/v1") {
		return url
	}
	return strings.TrimSuffix(url, "/") + "/v1"
}
