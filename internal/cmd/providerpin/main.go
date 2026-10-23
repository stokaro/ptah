// Command providerpin writes internal/providercatalog/pins_gen.go from the
// assets of official provider releases. It is the only code that reads a
// provider release; a regular build compiles the file it wrote and contacts
// nobody.
//
// Usage:
//
//	go run ./internal/cmd/providerpin --release ydb=0.1.1
//	go run ./internal/cmd/providerpin --release ydb=0.1.1 --assets /path/to/assets
//
// Every engine that has a release is named with its own --release flag; the
// file holds exactly the releases named. Without --assets the command downloads
// checksums.txt and the archives of each release from the engine's repository.
// With --assets it reads them from the directory, which must hold the files of
// every named release. A release that is missing a platform, lists a wrong
// digest, or holds an archive without the executable is refused.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/pflag"

	"ptah.run/internal/providercatalog"
	"ptah.run/internal/providerpin"
)

func main() {
	if err := run(context.Background(), os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "providerpin:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	flags := pflag.NewFlagSet("providerpin", pflag.ContinueOnError)
	releases := flags.StringArray("release", nil, "Release to pin as <engine>=<version>; repeat for each engine")
	assets := flags.String("assets", "", "Directory holding checksums.txt and the archives; downloaded from GitHub when empty")
	output := flags.String("output", filepath.Join("internal", "providercatalog", "pins_gen.go"), "File to write")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if len(*releases) == 0 {
		return errors.New("at least one --release <engine>=<version> is required")
	}
	dir := *assets
	if dir == "" {
		tmp, err := os.MkdirTemp("", "providerpin-")
		if err != nil {
			return err
		}
		defer func() { _ = os.RemoveAll(tmp) }()
		dir = tmp
	}
	var pins []providercatalog.Pin
	for _, spec := range *releases {
		engineID, version, found := strings.Cut(spec, "=")
		entry, known := providercatalog.Lookup(engineID)
		if !found || !known || entry.EngineID != engineID || version == "" {
			return fmt.Errorf("--release %q: want <official engine>=<version>", spec)
		}
		if *assets == "" {
			if err := download(ctx, entry, version, dir); err != nil {
				return err
			}
		}
		pin, err := providerpin.Build(engineID, version, dir)
		if err != nil {
			return fmt.Errorf("%s %s: %w", engineID, version, err)
		}
		pins = append(pins, pin)
	}
	source, err := providerpin.Render(pins)
	if err != nil {
		return err
	}
	return os.WriteFile(*output, source, 0o644) // #nosec G306 -- generated source, world-readable like the rest of the tree
}

func download(ctx context.Context, entry providercatalog.Entry, version, dir string) error {
	pin := providercatalog.Pin{EngineID: entry.EngineID, Version: version, Tag: "v" + version}
	base := pin.ReleaseBaseURL(entry)
	names := []string{providerpin.ChecksumsFile}
	for _, platform := range providercatalog.Platforms() {
		names = append(names, providercatalog.ArchiveName(entry.EngineID, version, platform))
	}
	client := &http.Client{Timeout: 10 * time.Minute}
	for _, name := range names {
		if err := fetch(ctx, client, base+"/"+name, filepath.Join(dir, name)); err != nil {
			return err
		}
	}
	return nil
}

func fetch(ctx context.Context, client *http.Client, address, path string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, address, http.NoBody)
	if err != nil {
		return err
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: %s", address, response.Status)
	}
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	if _, err := io.Copy(file, response.Body); err != nil {
		_ = file.Close()
		return fmt.Errorf("GET %s: %w", address, err)
	}
	return file.Close()
}
