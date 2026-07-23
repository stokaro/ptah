package atlasreport

import (
	"net/url"
	"strings"
)

type atlasEnv struct {
	Driver string `json:"Driver,omitempty"`
	URL    string `json:"URL,omitempty"`
	Dir    string `json:"Dir,omitempty"`
}

func atlasRedactedURL(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return ""
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
	return parsed.String()
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
