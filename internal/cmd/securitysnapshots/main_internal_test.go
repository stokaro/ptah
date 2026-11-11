package main

// White-box testing required: the source updater's review decision is private
// to this command and has no exported API.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/vulnerability"
)

func TestChangedVersionedSourceRequiresReview(t *testing.T) {
	c := qt.New(t)
	t.Chdir(t.TempDir())
	source := []byte("---\nrevision: sample\n---\n# Security changelog\nCVE-2026-10549\nCVE-2023-45825\nCVE-2027-12345\nCVE-2027-12345\n")
	now := time.Date(2027, 1, 2, 12, 0, 0, 0, time.UTC)
	c.Assert(updateVersioned("ydb", source, now), qt.IsNil)
	data, err := os.ReadFile(filepath.Join("internal", "vulnerability", "review", "ydb.json"))
	c.Assert(err, qt.IsNil)
	var pending review
	c.Assert(json.Unmarshal(data, &pending), qt.IsNil)
	current, err := vulnerability.ReadVersionedSnapshot("ydb", "")
	c.Assert(err, qt.IsNil)
	digest := sha256.Sum256(source)
	c.Assert(pending.ReviewedSHA, qt.Equals, current.SourceSHA256)
	c.Assert(pending.ObservedSHA, qt.Equals, hex.EncodeToString(digest[:]))
	c.Assert(pending.AddedIDs, qt.DeepEquals, []string{"CVE-2027-12345"})
	c.Assert(pending.MissingIDs, qt.DeepEquals, []string{"CVE-2022-28228"})
	c.Assert(pending.ObservedIDs, qt.DeepEquals, []string{"CVE-2023-45825", "CVE-2026-10549", "CVE-2027-12345"})
	c.Assert(updateVersioned("ydb", source, now.Add(24*time.Hour)), qt.IsNil)
	second, err := os.ReadFile(filepath.Join("internal", "vulnerability", "review", "ydb.json"))
	c.Assert(err, qt.IsNil)
	c.Assert(second, qt.DeepEquals, data)
	_, err = os.Stat(filepath.Join("internal", "vulnerability", "ydb.json"))
	c.Assert(err, qt.IsNotNil)
}

func TestSourceReviewRefusesEmptyCVEList(t *testing.T) {
	c := qt.New(t)
	current, err := vulnerability.ReadVersionedSnapshot("ydb", "")
	c.Assert(err, qt.IsNil)
	_, err = buildReview(current, []byte("---\nrevision: sample\n---\n# Security changelog\nVendor page without advisory records"), "abc", time.Now())
	c.Assert(err, qt.ErrorMatches, `changed source contains no CVE IDs`)
}

func TestYDBDocumentationRevisionDoesNotChangeAdvisoryContent(t *testing.T) {
	c := qt.New(t)
	before := []byte("---\nrevision: first\n---\n# Security changelog\n## Fixed in YDB 25.3.1.25\nCVE-2026-10549\n")
	after := []byte("---\nrevision: second\n---\n# Security changelog\n## Fixed in YDB 25.3.1.25\nCVE-2026-10549\n")
	changed := []byte("---\nrevision: second\n---\n# Security changelog\n## Fixed in YDB 25.3.1.26\nCVE-2026-10549\n")
	contentBefore, err := advisoryContent("ydb", before)
	c.Assert(err, qt.IsNil)
	contentAfter, err := advisoryContent("ydb", after)
	c.Assert(err, qt.IsNil)
	contentChanged, err := advisoryContent("ydb", changed)
	c.Assert(err, qt.IsNil)
	beforeSHA := sha256.Sum256(before)
	afterSHA := sha256.Sum256(after)
	contentSHA := sha256.Sum256(contentBefore)
	afterContentSHA := sha256.Sum256(contentAfter)
	changedContentSHA := sha256.Sum256(contentChanged)
	current := vulnerability.VersionedSnapshot{
		SourceSHA256:          hex.EncodeToString(beforeSHA[:]),
		AdvisoryContentSHA256: hex.EncodeToString(contentSHA[:]),
	}
	c.Assert(sameReviewedContent(current, hex.EncodeToString(afterSHA[:]), hex.EncodeToString(afterContentSHA[:])), qt.IsTrue)
	c.Assert(sameReviewedContent(current, hex.EncodeToString(afterSHA[:]), hex.EncodeToString(changedContentSHA[:])), qt.IsFalse)
	_, err = advisoryContent("ydb", []byte("vendor page without the security heading"))
	c.Assert(err, qt.ErrorMatches, `YDB security changelog heading missing`)
}

func TestRecentSourceNeedsPastRetrievalWithinRefreshInterval(t *testing.T) {
	c := qt.New(t)
	now := time.Date(2027, 1, 2, 12, 0, 0, 0, time.UTC)
	c.Assert(recent(now.Add(-13*24*time.Hour), now), qt.IsTrue)
	c.Assert(recent(now.Add(-refreshInterval), now), qt.IsFalse)
	c.Assert(recent(now.Add(time.Hour), now), qt.IsFalse)
}
