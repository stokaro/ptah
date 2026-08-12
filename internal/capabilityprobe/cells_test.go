package capabilityprobe_test

import (
	"fmt"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/internal/capabilityline"
	"go.5x5.cz/ptah/internal/capabilityprobe"
)

func TestCells_IsNotEmpty(t *testing.T) {
	c := qt.New(t)

	// A matrix with no cells covers no line, and every run against it would
	// report a line the matrix does not know. Asserting the list is populated
	// is the same refusal the repository's shell gates make when their input
	// list comes back empty.
	c.Assert(len(capabilityprobe.Cells) > 0, qt.IsTrue)
}

// TestCells_UseExactlyTheSharedMeasuredLines keeps the resolver's exact-line
// attribution and the matrix declaration tied in both directions. A shared
// line without a cell and a cell added without a shared resolver identifier
// both fail here rather than creating a second drifting release-line list.
func TestCells_UseExactlyTheSharedMeasuredLines(t *testing.T) {
	c := qt.New(t)

	for dialect, lines := range map[string][]string{
		platform.MySQL:       capabilityline.MySQLMeasured(),
		platform.MariaDB:     capabilityline.MariaDBMeasured(),
		platform.CockroachDB: capabilityline.CockroachDBMeasured(),
	} {
		declared := declaredLines(dialect)
		c.Assert(declared, qt.ContentEquals, lines,
			qt.Commentf("the matrix cells and resolver identifiers must name exactly the same %s lines", dialect))
		for _, line := range lines {
			c.Run(dialect+" "+line, func(c *qt.C) {
				version, err := capabilityprobe.ParseVersion(dialect, line, "")
				c.Assert(err, qt.IsNil)
				cell, found := capabilityprobe.CellFor(dialect, version)
				c.Assert(found, qt.IsTrue)
				c.Assert(cell.Line, qt.Equals, line)
			})
		}
	}
}

func declaredLines(dialect string) []string {
	cells := slices.DeleteFunc(slices.Clone(capabilityprobe.Cells), func(cell capabilityprobe.Cell) bool {
		return cell.Dialect != dialect
	})
	return slices.Collect(func(yield func(string) bool) {
		for _, cell := range cells {
			if !yield(cell.Line) {
				return
			}
		}
	})
}

func TestCells_AreWellFormed(t *testing.T) {
	c := qt.New(t)

	seen := map[string]bool{}
	for _, cell := range capabilityprobe.Cells {
		c.Run(cell.String(), func(c *qt.C) {
			c.Assert(platform.NormalizeDialect(cell.Dialect), qt.Equals, cell.Dialect,
				qt.Commentf("a cell dialect must already be normalized or CellFor will never match it"))
			c.Assert(cell.Line, qt.Not(qt.Equals), "")

			key := cell.Dialect + "/" + cell.Line
			c.Assert(seen[key], qt.IsFalse, qt.Commentf("two cells claim %s; the first one found would always win", key))
			seen[key] = true

			c.Assert(cell.Refinement, qt.Not(qt.Equals), capabilityprobe.Refinement(""))
		})
	}
}

// TestCells_MeasuredCellsNameAValidPreset checks the half of a cell a reader
// cannot verify by eye: that PresetName describes the set Preset returns, and
// that the set is one the registry accepts.
func TestCells_MeasuredCellsNameAValidPreset(t *testing.T) {
	c := qt.New(t)

	named := map[string]func() capability.Capabilities{
		"Postgres17":      capability.Postgres17,
		"Postgres16":      capability.Postgres16,
		"Postgres13":      capability.Postgres13,
		"MySQL84":         capability.MySQL84,
		"MariaDB1011":     capability.MariaDB1011,
		"ClickHouse24":    capability.ClickHouse24,
		"SQLite3":         capability.SQLite3,
		"SQLServer2022":   capability.SQLServer2022,
		"CockroachDB23":   capability.CockroachDB23,
		"CockroachDB25":   capability.CockroachDB25,
		"CockroachDB26":   capability.CockroachDB26,
		"YugabyteDB25":    capability.YugabyteDB25,
		"SpannerPostgres": capability.SpannerPostgres,
	}
	for _, cell := range capabilityprobe.Cells {
		c.Run(cell.String(), func(c *qt.C) {
			c.Assert(cell.Measured(), qt.Equals, cell.PresetName != "",
				qt.Commentf("a cell either names a preset and has one, or names neither"))
			for _, check := range measuredChecks(cell, named) {
				check(c)
			}
		})
	}
}

// measuredChecks returns the assertions that only apply to a cell that names a
// preset, so the loop body above stays free of a conditional.
func measuredChecks(cell capabilityprobe.Cell, named map[string]func() capability.Capabilities) []func(*qt.C) {
	if !cell.Measured() {
		return nil
	}
	return []func(*qt.C){
		func(c *qt.C) {
			build, known := named[cell.PresetName]
			c.Assert(known, qt.IsTrue, qt.Commentf("cell names preset %q, which this test does not know", cell.PresetName))
			c.Assert(cell.Preset(), qt.DeepEquals, build(),
				qt.Commentf("cell names preset %q but carries a different set", cell.PresetName))
			c.Assert(cell.Preset().Validate(), qt.IsNil)
		},
	}
}

func TestCellFor(t *testing.T) {
	c := qt.New(t)

	for _, tc := range []struct {
		name    string
		dialect string
		version string
		assert  func(c *qt.C, cell capabilityprobe.Cell, found bool)
	}{{
		name: "a PostgreSQL 17 patch release lands on the 17 line", dialect: platform.Postgres, version: "17.10",
		assert: func(c *qt.C, cell capabilityprobe.Cell, found bool) {
			c.Assert(found, qt.IsTrue)
			c.Assert(cell.Line, qt.Equals, "17")
			c.Assert(cell.PresetName, qt.Equals, "Postgres17")
		},
	}, {
		name: "a MySQL LTS line is matched on major and minor", dialect: platform.MySQL, version: "9.7.1",
		assert: func(c *qt.C, cell capabilityprobe.Cell, found bool) {
			c.Assert(found, qt.IsTrue)
			c.Assert(cell.Line, qt.Equals, "9.7")
		},
	}, {
		name: "a MySQL release on no LTS line matches nothing", dialect: platform.MySQL, version: "9.6.1",
		assert: func(c *qt.C, cell capabilityprobe.Cell, found bool) {
			c.Assert(found, qt.IsFalse)
			c.Assert(cell.Line, qt.Equals, "")
		},
	}, {
		// The Postgres13 preset's own doc covers PostgreSQL 12 and 13, and the
		// matrix declares only 13 because only 13 was probed. A 12 server must
		// therefore fall off the matrix rather than borrow the 13 cell's
		// result, or the matrix certifies a line nobody measured.
		name: "a PostgreSQL major the matrix does not declare matches nothing", dialect: platform.Postgres, version: "12.22",
		assert: func(c *qt.C, cell capabilityprobe.Cell, found bool) {
			c.Assert(found, qt.IsFalse)
		},
	}, {
		name:    "SQL Server matches on the product version, not the marketing year",
		dialect: platform.SQLServer, version: "17.0.4065.4",
		assert: func(c *qt.C, cell capabilityprobe.Cell, found bool) {
			c.Assert(found, qt.IsTrue)
			c.Assert(cell.Label, qt.Equals, "SQL Server 2025")
		},
	}, {
		name: "the marketing year matches no SQL Server cell", dialect: platform.SQLServer, version: "2025",
		assert: func(c *qt.C, cell capabilityprobe.Cell, found bool) {
			c.Assert(found, qt.IsFalse)
		},
	}, {
		name: "a dialect with no cells matches nothing", dialect: "nonsense", version: "1.2.3",
		assert: func(c *qt.C, cell capabilityprobe.Cell, found bool) {
			c.Assert(found, qt.IsFalse)
		},
	}} {
		c.Run(tc.name, func(c *qt.C) {
			version, err := capabilityprobe.ParseVersion(tc.dialect, tc.version, "")
			c.Assert(err, qt.IsNil)
			cell, found := capabilityprobe.CellFor(tc.dialect, version)
			tc.assert(c, cell, found)
		})
	}
}

// TestCells_CoverEveryVersionMeasuredFromALiveServer keeps the matrix honest
// about servers somebody actually started: each row below is a version string a
// live container reported, and a version with no cell is a server whose preset
// nothing here can describe.
//
// This is the half of the question a container tag cannot answer. A tag is
// vendor text — mcr.microsoft.com/mssql/server:2025-latest names a marketing
// year and clickhouse/clickhouse-server:26 names nothing precise at all — so
// TestCells_DeclareEveryDatabaseContainerThisRepositoryStarts checks that the
// matrix declares the containers, and this one checks that what those
// containers report on the wire lands on a cell.
func TestCells_CoverEveryVersionMeasuredFromALiveServer(t *testing.T) {
	c := qt.New(t)

	for _, tc := range []struct {
		dialect string
		version string
	}{
		{platform.Postgres, "18.4"},
		{platform.Postgres, "17.10"},
		{platform.MySQL, "26.7.0"},
		{platform.MySQL, "9.7.1"},
		{platform.MariaDB, "10.11.18"},
		{platform.MariaDB, "11.4.12"},
		{platform.MariaDB, "12.3.2"},
		{platform.ClickHouse, "26.7.3.19"},
		{platform.ClickHouse, "24.10.4.191"},
		{platform.CockroachDB, "26.2.5"},
		{platform.YugabyteDB, "2026.1.0.0"},
		{platform.SQLServer, "17.0.4065.4"},
	} {
		c.Run(fmt.Sprintf("%s %s", tc.dialect, tc.version), func(c *qt.C) {
			version, err := capabilityprobe.ParseVersion(tc.dialect, tc.version, "")
			c.Assert(err, qt.IsNil)
			_, found := capabilityprobe.CellFor(tc.dialect, version)
			c.Assert(found, qt.IsTrue)
		})
	}
}

// pinnedImageFiles are the two files that decide which database containers this
// repository starts: docker-compose.yaml for local runs and `make db-start`,
// the integration workflow for CI.
//
// Reading them is the point. The list this test used to carry was written by
// hand, and when the MySQL service moved from 9.7 to 26.7 the list kept naming
// 9.7 — so the guard that exists to catch an undeclared line went on passing
// while the only MySQL this repository starts fell off the matrix entirely.
var pinnedImageFiles = []string{
	"../../docker-compose.yaml",
	"../../.github/workflows/go-integration-tests.yml",
}

// imageLine matches a YAML `image:` entry at any indentation. Compose nests it
// under services.<name> and the workflow under jobs.<job>.services.<name>, so a
// structure-aware read would need two shapes to answer one question.
var imageLine = regexp.MustCompile(`(?m)^\s*image:\s*["']?([^"'\s#]+)`)

// databaseImages maps an image repository onto the dialect a server built from
// it speaks.
var databaseImages = map[string]string{
	"postgres":                       platform.Postgres,
	"mysql":                          platform.MySQL,
	"mariadb":                        platform.MariaDB,
	"clickhouse/clickhouse-server":   platform.ClickHouse,
	"cockroachdb/cockroach":          platform.CockroachDB,
	"yugabytedb/yugabyte":            platform.YugabyteDB,
	"mcr.microsoft.com/mssql/server": platform.SQLServer,
}

// notADatabase lists the images these files start that no capability preset
// describes. It is written out rather than inferred so that an image in neither
// map fails the test: a check that quietly ignores what it does not recognize
// would let the next database arrive with no cell and nothing to say so.
var notADatabase = map[string]bool{
	"registry": true,
}

// TestCells_DeclareEveryDatabaseContainerThisRepositoryStarts derives the
// coverage question from the files that answer it.
func TestCells_DeclareEveryDatabaseContainerThisRepositoryStarts(t *testing.T) {
	c := qt.New(t)

	pinned := readPinnedImages(c)
	assertThePinnedListHasDatabasesInIt(c, pinned)

	for _, ref := range pinned {
		c.Run(ref, func(c *qt.C) {
			for _, check := range checksForPinnedImage(ref) {
				check(c)
			}
		})
	}
}

func TestCellsDeclaring_LineAliasMatchesTheTargetLine(t *testing.T) {
	c := qt.New(t)

	c.Assert(cellsDeclaring("cockroachdb/cockroach", "v26.2.5"), qt.HasLen, 1)
	c.Assert(cellsDeclaring("cockroachdb/cockroach", "v27.1.0"), qt.HasLen, 0,
		qt.Commentf("a floating 25.4 or 26.2 cell must not cover an undeclared 27.1 target"))
}

// readPinnedImages returns every image reference the two files start, sorted
// and deduplicated: postgres:18 appears in both.
func readPinnedImages(c *qt.C) []string {
	c.Helper()

	var refs []string
	for _, path := range pinnedImageFiles {
		body, err := os.ReadFile(path)
		c.Assert(err, qt.IsNil)
		matches := imageLine.FindAllStringSubmatch(string(body), -1)
		c.Assert(len(matches) > 0, qt.IsTrue,
			qt.Commentf("%s yielded no image: lines — a check whose input list comes back empty passes "+
				"by examining nothing", path))
		for _, match := range matches {
			refs = append(refs, match[1])
		}
	}
	return slices.Compact(slices.Sorted(slices.Values(refs)))
}

// assertThePinnedListHasDatabasesInIt is the non-vacuity guard on the read
// above. A regex that stopped matching the service blocks would still return
// the workflow's registry image, every per-image check would classify it as not
// a database, and the test would pass having examined no database at all.
func assertThePinnedListHasDatabasesInIt(c *qt.C, pinned []string) {
	c.Helper()

	seen := map[string]bool{}
	for _, ref := range pinned {
		repository, _ := splitImageRef(ref)
		seen[databaseImages[repository]] = true
	}
	for _, dialect := range []string{platform.Postgres, platform.MySQL, platform.MariaDB} {
		c.Assert(seen[dialect], qt.IsTrue,
			qt.Commentf("no %s container was found in %v; the image read is broken, not the matrix", dialect, pinnedImageFiles))
	}
}

// checksForPinnedImage returns the assertions for one image reference, so the
// loop body stays free of a conditional.
func checksForPinnedImage(ref string) []func(*qt.C) {
	repository, tag := splitImageRef(ref)
	if notADatabase[repository] {
		return []func(*qt.C){func(c *qt.C) {
			c.Assert(cellsDeclaring(repository, tag), qt.HasLen, 0,
				qt.Commentf("%s is listed as not a database, yet a matrix cell claims it", ref))
		}}
	}
	dialect, known := databaseImages[repository]
	if !known {
		return []func(*qt.C){func(c *qt.C) {
			c.Fatalf("image %q is started by %v and appears in neither databaseImages nor notADatabase; "+
				"classify it rather than letting it through unexamined", ref, pinnedImageFiles)
		}}
	}
	return []func(*qt.C){func(c *qt.C) {
		declaring := cellsDeclaring(repository, tag)
		c.Assert(len(declaring) > 0, qt.IsTrue,
			qt.Commentf("this repository starts %s and no matrix cell declares it, so nothing here can "+
				"describe the capabilities of the server it runs (add a cell in cells.go)", ref))
		for _, cell := range declaring {
			c.Check(cell.Dialect, qt.Equals, dialect,
				qt.Commentf("cell %s declares image %q, whose repository speaks %s", cell, cell.Image, dialect))
		}
	}}
}

// cellsDeclaring returns the cells whose Image covers a pinned reference.
//
// Exact equality is the ordinary case. The single loosening is the floating
// tag: docker-compose.yaml pins clickhouse/clickhouse-server:26, which resolves
// to whichever 26.x the registry serves that day, so it is covered when the
// matrix declares a line beneath it. The prefix must end at a dot, or "2" would
// cover "26.7".
func cellsDeclaring(repository, tag string) []capabilityprobe.Cell {
	var out []capabilityprobe.Cell
	for _, cell := range capabilityprobe.Cells {
		cellRepository, cellTag := splitImageRef(cell.Image)
		if cellRepository != repository {
			continue
		}
		lineAlias := cellTag == "latest-v"+cell.Line && tagNamesLine(tag, cell.Line)
		resolvedLine := cell.ResolveNewestPatch && strings.HasPrefix(tag, cell.Line+".")
		if cellTag == tag || strings.HasPrefix(cellTag, tag+".") || lineAlias || resolvedLine {
			out = append(out, cell)
		}
	}
	return out
}

func tagNamesLine(tag, line string) bool {
	normalized := strings.TrimPrefix(strings.TrimPrefix(tag, "latest-v"), "v")
	return normalized == line || strings.HasPrefix(normalized, line+".")
}

// splitImageRef separates an image reference into repository and tag. The colon
// is taken from the right and only counts after the last slash, so a registry
// host carrying a port is not mistaken for a tag.
func splitImageRef(ref string) (repository, tag string) {
	colon := strings.LastIndex(ref, ":")
	if colon <= strings.LastIndex(ref, "/") {
		return ref, ""
	}
	return ref[:colon], ref[colon+1:]
}
