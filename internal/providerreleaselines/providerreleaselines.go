// Package providerreleaselines holds the release lines an external provider
// declares for its engine, pinned by version and digest.
//
// A provider publishes release-lines.json beside each release and lists its
// SHA-256 in the release's checksums.txt. The host keeps the copy of the
// release it supports here, so the support matrix can be compared with what
// the provider itself declares without starting the provider. To move to a new
// release, download release-lines.json from it, check the digest against the
// release's checksums.txt, replace the embedded file, and update the constants.
package providerreleaselines

import (
	"crypto/sha256"
	_ "embed" // The pinned document is part of the package.
	"encoding/hex"
	"encoding/json"
	"fmt"

	"ptah.run/core/platform/capability"
)

// YDBPluginVersion is the ptah-ydb-plugin release the embedded document comes
// from.
const YDBPluginVersion = "v0.1.1"

// YDBDigest is the SHA-256 checksums.txt of that release lists for the
// document.
const YDBDigest = "7bde5b63304fb92d64b978f6774e084ad7674d8fb448559a0e7023257509cd1a"

//go:embed ydb-release-lines.json
var ydbDocument []byte

// Line is one release line of an engine.
type Line struct {
	Line     string `json:"line"`
	Label    string `json:"label"`
	Support  string `json:"support"`
	Image    string `json:"image"`
	Unprobed string `json:"unprobed"`
	Note     string `json:"note"`
}

// Engine is the set of release lines one engine declares.
type Engine struct {
	ID    string `json:"engine"`
	Lines []Line `json:"lines"`
}

// Document is a provider's release-lines.json.
type Document struct {
	Provider string   `json:"provider"`
	Engines  []Engine `json:"engines"`
}

// YDB returns the pinned YDB document. It refuses a document whose digest is
// not the pinned one, so an edit to the embedded file cannot pass unnoticed.
func YDB() (Document, error) {
	sum := sha256.Sum256(ydbDocument)
	if got := hex.EncodeToString(sum[:]); got != YDBDigest {
		return Document{}, fmt.Errorf("embedded YDB release lines have digest %s, pinned %s", got, YDBDigest)
	}
	var document Document
	if err := json.Unmarshal(ydbDocument, &document); err != nil {
		return Document{}, fmt.Errorf("parse embedded YDB release lines: %w", err)
	}
	return document, nil
}

// SupportLevel maps the provider protocol's level name to the host's.
func SupportLevel(name string) (capability.SupportLevel, error) {
	switch name {
	case "certified":
		return capability.Certified, nil
	case "legacy_tested":
		return capability.LegacyTested, nil
	case "best_effort":
		return capability.BestEffort, nil
	case "known_incompatible":
		return capability.KnownIncompatible, nil
	}
	return "", fmt.Errorf("unknown release line support level %q", name)
}
