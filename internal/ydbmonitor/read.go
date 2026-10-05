// Package ydbmonitor reads authenticated metadata from an explicitly configured
// YDB monitoring endpoint. Flags and column-table descriptions share its bounds
// and redirect policy so neither path can leak the database credential.
package ydbmonitor

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// timeout bounds one read. The client applies it whatever the caller's
// context says, and the earlier of the two ends the read.
const timeout = 30 * time.Second

// maxBody bounds the page Ptah reads. A database lists a few hundred flags,
// about 15 KB measured.
const maxBody = 4 << 20

// maxEcho bounds how much of a refusing page an error repeats: enough for the
// endpoint's own message, and not the whole of whatever answered.
const maxEcho = 256

// client follows no redirect. The page is read from the endpoint the operator
// named, and a redirect to another host is answered as the status it is
// rather than followed somewhere Ptah was not pointed at.
var client = &http.Client{
	Timeout: timeout,
	CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	},
}

// Read fetches one JSON metadata page. The ticket uses YDB's bare Authorization
// header. Redirects are refused, response sizes are bounded and error bodies
// redact the ticket. resource describes the requested metadata in diagnostics.
func Read(ctx context.Context, endpoint *url.URL, resource, path, database, ticket string, query url.Values) ([]byte, error) {
	if endpoint == nil {
		return nil, fmt.Errorf("no monitoring endpoint to read %s from", resource)
	}
	page := *endpoint
	page.Path, page.RawPath, page.Fragment = path, "", ""
	page.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, page.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("read YDB %s: %w", resource, err)
	}
	request.Header.Set("Accept", "application/json")
	if ticket != "" {
		request.Header.Set("Authorization", ticket)
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("read YDB %s from %s: %w", resource, page.Redacted(), err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, maxBody+1))
	if err != nil {
		return nil, fmt.Errorf("read YDB %s from %s: %w", resource, page.Redacted(), err)
	}
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("read YDB %s from %s: %s: %s%s", resource, page.Redacted(), response.Status, echo(body, ticket), rightsHint(response.StatusCode, ticket, database))
	}
	if len(body) > maxBody {
		return nil, fmt.Errorf("read YDB %s from %s: response exceeds %d bytes", resource, page.Redacted(), maxBody)
	}
	return body, nil
}

// echo is the start of a refusing page, as an error repeats it. An endpoint
// that repeats the request's headers would repeat the ticket, so the ticket is
// taken out before the page is cut.
func echo(body []byte, ticket string) string {
	text := strings.TrimSpace(string(body))
	if ticket != "" {
		text = strings.ReplaceAll(text, ticket, "<redacted>")
	}
	if len(text) <= maxEcho {
		return text
	}
	return strings.ToValidUTF8(text[:maxEcho], "") + "..."
}

// rightsHint says what a 400 means when the page was read with a credential:
// measured on 26.2.1.14, the endpoint answers `Failed to resolve database` to
// a user without DESCRIBE SCHEMA on the database, the same user reads the
// page once granted it, and anonymous reads on a cluster that does not
// enforce authentication are not checked at all.
func rightsHint(status int, ticket, database string) string {
	if status != http.StatusBadRequest || ticket == "" {
		return ""
	}
	return fmt.Sprintf("; the page is read as the connection's user, who needs DESCRIBE SCHEMA on %s", database)
}
