package ociartifact_test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/ociartifact"
	"go.5x5.cz/ptah/internal/testutils"
)

func TestClient_RegistryOperationTimeout(t *testing.T) {
	c := qt.New(t)
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		<-request.Context().Done()
	}))
	defer server.Close()
	host := registryHost(c, server.URL)
	client, err := ociartifact.NewClient(ociartifact.ClientOptions{
		PlainHTTP:        true,
		OperationTimeout: 50 * time.Millisecond,
	})
	c.Assert(err, qt.IsNil)
	startedAt := time.Now()

	_, err = client.Pull(
		t.Context(),
		"oci://"+host+"/acme/migrations:latest",
		ociartifact.PullOptions{},
	)

	c.Assert(err, qt.IsNotNil)
	c.Assert(time.Since(startedAt) < 2*time.Second, qt.IsTrue)
}

func TestClient_CredentialHelperTimeout(t *testing.T) {
	testutils.SkipWithoutPOSIXShell(t)
	c := qt.New(t)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("WWW-Authenticate", `Basic realm="ptah-test"`)
		response.WriteHeader(http.StatusUnauthorized)
	}))
	defer server.Close()
	host := registryHost(c, server.URL)
	configDir := t.TempDir()
	helperDir := t.TempDir()
	c.Assert(
		os.WriteFile(
			filepath.Join(configDir, "config.json"),
			[]byte(`{"credHelpers":{"`+host+`":"ptahhang"}}`),
			0o600,
		),
		qt.IsNil,
	)
	c.Assert(
		os.WriteFile( // #nosec G306 -- Docker credential helpers must be executable.
			filepath.Join(helperDir, "docker-credential-ptahhang"),
			[]byte("#!/bin/sh\nexec sleep 10\n"),
			0o700,
		),
		qt.IsNil,
	)
	t.Setenv("DOCKER_CONFIG", configDir)
	t.Setenv("PATH", helperDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	client, err := ociartifact.NewClient(ociartifact.ClientOptions{
		PlainHTTP:        true,
		OperationTimeout: 50 * time.Millisecond,
	})
	c.Assert(err, qt.IsNil)
	startedAt := time.Now()

	_, err = client.Pull(
		t.Context(),
		"oci://"+host+"/acme/migrations:latest",
		ociartifact.PullOptions{},
	)

	c.Assert(err, qt.IsNotNil)
	c.Assert(time.Since(startedAt) < 2*time.Second, qt.IsTrue)
}

func registryHost(c *qt.C, rawURL string) string {
	parsed, err := url.Parse(rawURL)
	c.Assert(err, qt.IsNil)
	return parsed.Host
}
