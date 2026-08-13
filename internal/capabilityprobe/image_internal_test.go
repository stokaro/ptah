package capabilityprobe

// White-box testing required: Docker Hub pagination, numeric tag ordering, and
// selector replacement are unexported invariants behind ResolveDockerRun; a
// black-box test would require live registry traffic and could not isolate them.

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestResolveDockerHubImageSelectsNewestPatchAcrossPages(t *testing.T) {
	c := qt.New(t)
	bodies := map[string]string{
		"1": `{"next":"next page","results":[{"name":"2025.2.4.1-b4"},{"name":"2025.2-preview"}]}`,
		"2": `{"next":"","results":[{"name":"2025.2.5.0-b122"},{"name":"2025.2.5.1-b1"},{"name":"2026.1.0.1-b1"}]}`,
	}

	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		c.Check(request.URL.Path, qt.Equals, "/yugabytedb/yugabyte/tags")
		c.Check(request.URL.Query().Get("name"), qt.Equals, "2025.2.")
		body := bodies[request.URL.Query().Get("page")]
		c.Assert(body, qt.Not(qt.Equals), "", qt.Commentf("the resolver requested an undeclared page"))
		return jsonResponse(body), nil
	})}

	got, err := resolveDockerHubImage(
		context.Background(), client, "https://hub.example.test",
		"yugabytedb/yugabyte:2025.2", "2025.2",
	)
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Equals, "yugabytedb/yugabyte:2025.2.5.1-b1")
}

func TestResolveDockerHubImageRefusesAnUnresolvedLine(t *testing.T) {
	c := qt.New(t)

	client := &http.Client{Transport: roundTripFunc(func(_ *http.Request) (*http.Response, error) {
		return jsonResponse(`{"next":"","results":[{"name":"2026.1.0.1-b1"},{"name":"2025.2-preview"}]}`), nil
	})}

	_, err := resolveDockerHubImage(
		context.Background(), client, "https://hub.example.test",
		"yugabytedb/yugabyte:2025.2", "2025.2",
	)
	c.Assert(err, qt.ErrorMatches, `Docker Hub repository yugabytedb/yugabyte has no numeric patch tag for release line 2025.2`)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

func jsonResponse(body string) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewBufferString(body)),
	}
}

func TestReplaceDockerImageReplacesOnlyTheImageSelector(t *testing.T) {
	c := qt.New(t)

	arguments := []string{
		"--publish", "5433:5433", "yugabytedb/yugabyte:2025.2", "bash", "-lc", "echo 2025.2",
	}
	got, err := replaceDockerImage(
		arguments,
		"yugabytedb/yugabyte:2025.2",
		"yugabytedb/yugabyte:2025.2.5.1-b1",
	)
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, []string{
		"--publish", "5433:5433", "yugabytedb/yugabyte:2025.2.5.1-b1", "bash", "-lc", "echo 2025.2",
	})
	c.Assert(arguments[2], qt.Equals, "yugabytedb/yugabyte:2025.2",
		qt.Commentf("resolution must not mutate the matrix declaration shared by other jobs"))
}
