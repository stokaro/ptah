// Command pgsecuritysnapshot updates the reviewed local PostgreSQL advisories.
// The scan command itself never fetches the source page.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"ptah.run/internal/vulnerability"
)

func main() {
	input := flag.String("input", "", "Saved PostgreSQL security HTML; empty fetches the upstream page")
	flag.Parse()
	var source []byte
	var err error
	if *input != "" {
		source, err = os.ReadFile(*input)
	} else {
		client := http.Client{Timeout: 30 * time.Second}
		request, requestErr := http.NewRequest(http.MethodGet, vulnerability.SourceURL, nil)
		if requestErr != nil {
			fail(requestErr)
		}
		response, requestErr := client.Do(request)
		if requestErr != nil {
			fail(requestErr)
		}
		defer func() { _ = response.Body.Close() }()
		if response.StatusCode != http.StatusOK {
			fail(fmt.Errorf("advisory source returned HTTP %d", response.StatusCode))
		}
		source, err = io.ReadAll(io.LimitReader(response.Body, 8<<20))
		if len(source) >= 8<<20 {
			fail(fmt.Errorf("advisory source exceeds 8 MiB"))
		}
	}
	if err != nil {
		fail(err)
	}
	snapshot, err := vulnerability.SnapshotFromHTML(source, time.Now().UTC())
	if err != nil {
		fail(err)
	}
	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		fail(err)
	}
	data = append(data, '\n')
	const output = "internal/vulnerability/postgresql.json"
	// #nosec G703 -- output is a constant path in the contributor's checkout; the fetched page supplies only file content.
	if err := os.WriteFile(output, data, 0o600); err != nil {
		fail(err)
	}
	fmt.Fprintf(os.Stderr, "wrote %d advisories for %v to %s\n", len(snapshot.Advisories), snapshot.ReleaseLines, output)
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
