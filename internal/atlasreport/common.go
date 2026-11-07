package atlasreport

import (
	"net/url"
	"path"
	"strings"

	"ptah.run/internal/atlasurl"
	"ptah.run/internal/dburldisplay"
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
	parsed, err := atlasurl.Parse(dburldisplay.Format(raw))
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
		if dburldisplay.IsSecretParameter(key) {
			query.Set(key, "xxxxx")
		}
	}
	parsed.RawQuery = query.Encode()
	result := atlasTemplateURL{URL: *parsed}
	// The schema Atlas names for an engine of one namespace, such as SQLite's
	// main, goes with a URL spelled with the dialect's own name: sqlite://
	// names it, and sqlite3:// and file: name none.
	if fact := atlasFacts.Get(parsed.Scheme); fact.schema != "" && fact.name == parsed.Scheme {
		result.Schema = fact.schema
	}
	return result
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
