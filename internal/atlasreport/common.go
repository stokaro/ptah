package atlasreport

import (
	"net/url"
	"path"
	"strings"
)

type atlasEnv struct {
	Driver string           `json:"Driver,omitempty"`
	URL    atlasTemplateURL `json:"URL,omitzero"`
	Dir    string           `json:"Dir,omitempty"`
}

type atlasTemplateURL struct {
	url.URL
	Schema string `json:"Schema,omitempty"`
}

func (u atlasTemplateURL) String() string {
	return u.URL.String()
}

func (u atlasTemplateURL) IsZero() bool {
	return u.URL.String() == "" && u.Schema == ""
}

func atlasRedactedURL(raw string) atlasTemplateURL {
	parsed, err := url.Parse(raw)
	if err != nil {
		return atlasTemplateURL{}
	}
	if parsed.User != nil {
		username := parsed.User.Username()
		if username != "" {
			parsed.User = url.User(username)
		} else {
			parsed.User = nil
		}
	}
	query := parsed.Query()
	for key := range query {
		if isAtlasSensitiveQueryKey(key) {
			query.Set(key, "xxxxx")
		}
	}
	parsed.RawQuery = query.Encode()
	result := atlasTemplateURL{URL: *parsed}
	if parsed.Scheme == "sqlite" {
		result.Schema = "main"
	}
	return result
}

func isAtlasSensitiveQueryKey(key string) bool {
	normalized := strings.ToLower(key)
	compact := strings.NewReplacer("_", "", "-", "", ".", "").Replace(normalized)
	switch compact {
	case "password", "passwd", "pass", "pwd", "token", "secret", "accesstoken", "authtoken", "apikey", "clientsecret":
		return true
	default:
		return strings.Contains(compact, "password") ||
			strings.Contains(compact, "passwd") ||
			strings.Contains(compact, "token") ||
			strings.Contains(compact, "secret") ||
			strings.Contains(compact, "apikey")
	}
}

func atlasMigrationFileDescription(fileName string) string {
	stem := strings.TrimSuffix(path.Base(fileName), ".sql")
	stem = strings.TrimSuffix(stem, ".up")
	stem = strings.TrimSuffix(stem, ".down")
	_, description, ok := strings.Cut(stem, "_")
	if !ok {
		return ""
	}
	return description
}
