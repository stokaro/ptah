package shard

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
)

// Names of the two files a shard leaves in its report directory.
const (
	ManifestFile = "manifest.json"
	EventsFile   = "report.jsonl"
)

// LoadReports reads every shard report under dir, in any directory depth, so
// that one artifact per shard and one merged directory both load. A shard is a
// directory holding ManifestFile; the EventsFile beside it is its event
// stream, and an absent one reads as a shard that ran nothing. A dir that does
// not exist holds no report, and Verify refuses an empty set.
func LoadReports(dir string) ([]Report, error) {
	var manifests []string
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.IsDir() && entry.Name() == ManifestFile {
			manifests = append(manifests, path)
		}
		return nil
	})
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("find shard reports in %s: %w", dir, err)
	}
	slices.Sort(manifests)

	reports := make([]Report, 0, len(manifests))
	for _, path := range manifests {
		manifestBytes, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read shard manifest: %w", err)
		}
		manifest, err := ReadManifest(bytes.NewReader(manifestBytes))
		if err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		events, err := os.ReadFile(filepath.Join(filepath.Dir(path), EventsFile))
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("read shard report: %w", err)
		}
		reports = append(reports, Report{Manifest: manifest, Events: bytes.NewReader(events)})
	}
	return reports, nil
}
