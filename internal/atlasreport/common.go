package atlasreport

import (
	"net/url"

	"ptah.run/internal/atlasurl"
	"ptah.run/internal/dburldisplay"
	"ptah.run/internal/secretmask"
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

// atlasRedacted is what an Atlas-shaped URL writes in place of a value it
// hides. It needs no escape in any part of a URL, so a part that holds it
// keeps the shape it was parsed with.
const atlasRedacted = "xxxxx"

// atlasRedactedURL is raw as an Atlas-shaped URL: the display [dburldisplay]
// lays out, parsed, with the password dropped, each secret setting's value
// written as [atlasRedacted], and a hidden value taken out of each other part
// that repeats it. The parts are masked after the parse, so a port or a user
// that repeats the password keeps the URL whole.
func atlasRedactedURL(raw string) atlasTemplateURL {
	display, hidden := dburldisplay.Layout(raw)
	parsed, err := atlasurl.Parse(display)
	if err != nil {
		return atlasTemplateURL{}
	}
	if parsed.User != nil {
		username := hidden.Mask(parsed.User.Username(), atlasRedacted)
		if username != "" {
			parsed.User = url.User(username)
		} else {
			parsed.User = nil
		}
	}
	parsed.Host = hidden.Mask(parsed.Host, atlasRedacted)
	parsed.Path, parsed.RawPath = hidden.Mask(parsed.Path, atlasRedacted), ""
	parsed.Opaque = hidden.Mask(parsed.Opaque, atlasRedacted)
	parsed.Fragment, parsed.RawFragment = hidden.Mask(parsed.Fragment, atlasRedacted), ""
	query := parsed.Query()
	for key, values := range query {
		if secretmask.IsSecretParameter(key) {
			query.Set(key, atlasRedacted)
			continue
		}
		for i := range values {
			values[i] = hidden.Mask(values[i], atlasRedacted)
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
