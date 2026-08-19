package capabilityprobe_test

import (
	"encoding/json"
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
			t.Run(dialect+" "+line, func(t *testing.T) {
				c := qt.New(t)
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
	seen := make(map[string]bool)
	for _, cell := range capabilityprobe.Cells {
		t.Run(cell.String(), func(t *testing.T) {
			c := qt.New(t)
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
	// Read from the registry rather than written down here. The hand-written
	// map this replaces was a second list of presets, and it went stale in the
	// direction that passes: a cell naming a preset the map did not know was
	// the only way to notice, so the map was always updated one PR late
	// (stokaro/ptah#916).
	named := make(map[string]capability.Capabilities)
	for _, preset := range capability.NamedPresets() {
		named[preset.Name] = preset.Capabilities
	}
	for _, cell := range capabilityprobe.Cells {
		t.Run(cell.String(), func(t *testing.T) {
			c := qt.New(t)
			c.Assert(cell.Measured(), qt.Equals, cell.PresetName != "",
				qt.Commentf("a cell either names a preset and has one, or names neither"))
			assertMeasuredCell(c, cell, named)
		})
	}
}

// assertMeasuredCell checks what only applies to a cell that names a preset.
//
// The conditional lives here rather than in the loop body above, which is why
// this is a helper at all -- it used to be a helper returning a slice of
// closures, which is the same thing with a checker handed through an extra
// indirection nothing needed.
func assertMeasuredCell(c *qt.C, cell capabilityprobe.Cell, named map[string]capability.Capabilities) {
	c.Helper()
	if !cell.Measured() {
		return
	}
	want, known := named[cell.PresetName]
	c.Assert(known, qt.IsTrue,
		qt.Commentf("cell names preset %q, which capability.NamedPresets does not list", cell.PresetName))
	c.Assert(cell.Preset(), qt.DeepEquals, want,
		qt.Commentf("cell names preset %q but carries a different set", cell.PresetName))
	c.Assert(cell.Preset().Validate(), qt.IsNil)
}

// TestCellFor_LandsOnTheDeclaredLine covers the versions a cell claims. The
// label and the preset name are asserted beside the line because a cell reached
// by the right line but carrying another line's preset would certify
// capabilities nobody measured on that server.
func TestCellFor_LandsOnTheDeclaredLine(t *testing.T) {
	for _, tc := range []struct {
		name           string
		dialect        string
		version        string
		wantLine       string
		wantLabel      string
		wantPresetName string
	}{{
		name: "a PostgreSQL 17 patch release lands on the 17 line", dialect: platform.Postgres, version: "17.10",
		wantLine: "17", wantPresetName: "Postgres17",
	}, {
		name: "a MySQL LTS line is matched on major and minor", dialect: platform.MySQL, version: "9.7.1",
		wantLine: "9.7", wantPresetName: "MySQL84",
	}, {
		name:    "SQL Server matches on the product version, not the marketing year",
		dialect: platform.SQLServer, version: "17.0.4065.4",
		wantLine: "17.0", wantLabel: "SQL Server 2025", wantPresetName: "SQLServer2022",
	}} {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			version, err := capabilityprobe.ParseVersion(tc.dialect, tc.version, "")
			c.Assert(err, qt.IsNil)

			cell, found := capabilityprobe.CellFor(tc.dialect, version)

			c.Assert(found, qt.IsTrue)
			c.Assert(cell.Line, qt.Equals, tc.wantLine)
			c.Assert(cell.Label, qt.Equals, tc.wantLabel)
			c.Assert(cell.PresetName, qt.Equals, tc.wantPresetName)
		})
	}
}

// TestCellFor_FallsOffTheMatrix covers the versions no cell claims. Falling off
// is the honest answer: a server the matrix cannot describe must not borrow a
// neighboring line's result, so the returned cell has to stay empty rather than
// carry the nearest match.
func TestCellFor_FallsOffTheMatrix(t *testing.T) {
	for _, tc := range []struct {
		name    string
		dialect string
		version string
	}{{
		name: "a MySQL release on no LTS line", dialect: platform.MySQL, version: "9.6.1",
	}, {
		// The Postgres13 preset's own doc covers PostgreSQL 12 and 13, and the
		// matrix declares only 13 because only 13 was probed. A 12 server must
		// therefore fall off the matrix rather than borrow the 13 cell's
		// result, or the matrix certifies a line nobody measured.
		name: "a PostgreSQL major the matrix does not declare", dialect: platform.Postgres, version: "12.22",
	}, {
		name: "the marketing year of a SQL Server release", dialect: platform.SQLServer, version: "2025",
	}, {
		name: "a dialect with no cells at all", dialect: "nonsense", version: "1.2.3",
	}} {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			version, err := capabilityprobe.ParseVersion(tc.dialect, tc.version, "")
			c.Assert(err, qt.IsNil)

			cell, found := capabilityprobe.CellFor(tc.dialect, version)

			c.Assert(found, qt.IsFalse)
			c.Assert(cell.Line, qt.Equals, "")
			c.Assert(cell.Label, qt.Equals, "")
			c.Assert(cell.PresetName, qt.Equals, "")
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
		t.Run(fmt.Sprintf("%s %s", tc.dialect, tc.version), func(t *testing.T) {
			c := qt.New(t)
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

// dockerRun matches a `docker run` invocation and captures its arguments. The
// capture runs to the end of the shell command rather than to the end of the
// line, because the workflow's two invocations span four and three lines and
// every continued line ends in a backslash.
//
// A YAML `image:` key is not the only way this repository starts a database,
// and the difference was measured rather than imagined: the integration
// workflow starts CockroachDB and YugabyteDB from a `run:` step, and a reviewer
// added `docker run --detach --name ptah-extra-mysql --publish 3399:3306
// mysql:8.0` to that step -- MySQL 8.0 being a line the matrix deliberately
// declares no cell for -- and `go test ./internal/capabilityprobe/...` came
// back green. CI would have started a server nothing here can describe with
// nothing red, which is the exact failure reading these files prevents.
var dockerRun = regexp.MustCompile(`docker[ \t]+run((?:\\\n|[^\n])*)`)

// dockerRunValueFlags names the `docker run` flags whose value is the NEXT
// token rather than part of the same one. Walking past them is the whole
// difficulty of finding the image: `--publish 3399:3306` splits into a
// repository and a tag exactly as readily as `mysql:8.0` does, so a read that
// took the first token carrying a colon would classify a port mapping and miss
// the server entirely.
//
// A flag missing from this list is not silent. Its value is taken for the
// image, and an image neither databaseImages nor notADatabase classifies fails
// TestCells_DeclareEveryDatabaseContainerThisRepositoryStarts by name.
var dockerRunValueFlags = map[string]bool{
	"--name":       true,
	"--publish":    true,
	"-p":           true,
	"--env":        true,
	"-e":           true,
	"--env-file":   true,
	"--volume":     true,
	"-v":           true,
	"--network":    true,
	"--user":       true,
	"-u":           true,
	"--workdir":    true,
	"-w":           true,
	"--entrypoint": true,
	"--hostname":   true,
	"-h":           true,
	"--label":      true,
	"-l":           true,
	"--platform":   true,
	"--pull":       true,
	"--restart":    true,
	"--health-cmd": true,
	"--memory":     true,
	"-m":           true,
	"--cpus":       true,
}

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
	// The vendor's emulator behind PGAdapter, which speaks the Spanner
	// PostgreSQL interface. It is classified here as the server for the
	// dialect because that is what the matrix starts; the cell's Emulated flag
	// is what keeps it from being read as the managed service.
	"gcr.io/cloud-spanner-pg-adapter/pgadapter-emulator": platform.Spanner,
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
		t.Run(ref, func(t *testing.T) {
			assertPinnedImageIsClassified(qt.New(t), ref)
		})
	}
}

func TestCellsDeclaring_LineAliasMatchesTheTargetLine(t *testing.T) {
	c := qt.New(t)

	c.Assert(cellsDeclaring("cockroachdb/cockroach", "v26.2.5"), qt.HasLen, 1)
	c.Assert(cellsDeclaring("cockroachdb/cockroach", "v27.1.0"), qt.HasLen, 0,
		qt.Commentf("a floating 25.4 or 26.2 cell must not cover an undeclared 27.1 target"))
}

// TestDockerRun_FindsTheDatabasesTheWorkflowStartsByHand is the non-vacuity
// guard on the docker run half of the read, and it pins today's answer the way
// TestCells_BestEffortLinesAreExactlyTheUnmeasuredOnes pins today's levels.
//
// The `image:` half has a guard of its own inside startedImages; this half needs
// one because it can only ever return fewer images than the file starts. A regex
// that stopped matching, or an unlisted flag that swallowed the image token,
// would leave the census reading exactly as it did before this arm existed:
// passing, and blind to the servers CI starts by hand. Both mutants were run in
// a scratch copy and both turn this test red.
//
// Repositories rather than full references, so that a tag bump stays a change to
// cells.go alone.
func TestDockerRun_FindsTheDatabasesTheWorkflowStartsByHand(t *testing.T) {
	c := qt.New(t)

	body, err := os.ReadFile(integrationWorkflow)
	c.Assert(err, qt.IsNil)

	var repositories []string
	for _, ref := range dockerRunImages(string(body)) {
		repository, _ := splitImageRef(ref)
		repositories = append(repositories, repository)
	}
	c.Assert(repositories, qt.ContentEquals, []string{"cockroachdb/cockroach", "yugabytedb/yugabyte"})
}

// readPinnedImages returns every image reference the two files start, sorted
// and deduplicated: postgres:18 appears in both.
func readPinnedImages(c *qt.C) []string {
	c.Helper()

	var refs []string
	for _, path := range pinnedImageFiles {
		refs = append(refs, startedImages(c, path)...)
	}
	return slices.Compact(slices.Sorted(slices.Values(refs)))
}

// startedImages returns every container image one file starts, by both of the
// mechanisms these files use: the YAML `image:` key, and a `docker run`
// invocation in a workflow step. Compose can only use the first; the
// integration workflow uses both, and reading only the first is how the census
// came to be blind to two of the seven databases CI runs.
func startedImages(c *qt.C, path string) []string {
	c.Helper()

	body, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)

	matches := imageLine.FindAllStringSubmatch(string(body), -1)
	c.Assert(len(matches) > 0, qt.IsTrue,
		qt.Commentf("%s yielded no image: lines — a check whose input list comes back empty passes "+
			"by examining nothing", path))

	refs := make([]string, 0, len(matches))
	for _, match := range matches {
		refs = append(refs, match[1])
	}
	return append(refs, dockerRunImages(string(body))...)
}

// dockerRunImages returns the image every `docker run` in a file starts. An
// invocation whose image the walk below cannot find contributes nothing rather
// than an empty reference that would later read as an unclassified image;
// TestDockerRun_FindsTheDatabasesTheWorkflowStartsByHand is the guard against
// that silence.
func dockerRunImages(body string) []string {
	var out []string
	for _, invocation := range dockerRun.FindAllStringSubmatch(body, -1) {
		ref, found := dockerRunImage(strings.Fields(strings.ReplaceAll(invocation[1], "\\\n", " ")))
		if found {
			out = append(out, ref)
		}
	}
	return out
}

// dockerRunImage returns the image reference out of one invocation's arguments:
// the first token that is neither a flag nor a flag's value. Everything after
// it is the container's own command, which is why the walk stops there — the
// CockroachDB invocation continues `start-single-node --insecure`, and a read
// that kept going would classify a subcommand as an image.
func dockerRunImage(args []string) (ref string, found bool) {
	for i := 0; i < len(args); i++ {
		if !strings.HasPrefix(args[i], "-") {
			return args[i], true
		}
		if dockerRunValueFlags[args[i]] {
			i++
		}
	}
	return "", false
}

// assertThePinnedListHasDatabasesInIt is the non-vacuity guard on the read
// above. A regex that stopped matching the service blocks would still return
// the workflow's registry image, every per-image check would classify it as not
// a database, and the test would pass having examined no database at all.
func assertThePinnedListHasDatabasesInIt(c *qt.C, pinned []string) {
	c.Helper()

	seen := make(map[string]bool)
	for _, ref := range pinned {
		repository, _ := splitImageRef(ref)
		seen[databaseImages[repository]] = true
	}
	for _, dialect := range []string{platform.Postgres, platform.MySQL, platform.MariaDB} {
		c.Assert(seen[dialect], qt.IsTrue,
			qt.Commentf("no %s container was found in %v; the image read is broken, not the matrix", dialect, pinnedImageFiles))
	}
}

// assertPinnedImageIsClassified checks one image reference against the matrix.
//
// The three answers -- not a database, unclassified, or a database that needs a
// cell -- are branches, and they live here so the loop body does not carry
// them. This used to return a slice of closures per branch, which handed the
// checker through an indirection that bought nothing.
func assertPinnedImageIsClassified(c *qt.C, ref string) {
	c.Helper()
	repository, tag := splitImageRef(ref)
	if notADatabase[repository] {
		c.Assert(cellsDeclaring(repository, tag), qt.HasLen, 0,
			qt.Commentf("%s is listed as not a database, yet a matrix cell claims it", ref))
		return
	}
	dialect, known := databaseImages[repository]
	if !known {
		c.Fatalf("image %q is started by %v and appears in neither databaseImages nor notADatabase; "+
			"classify it rather than letting it through unexamined", ref, pinnedImageFiles)
		return
	}
	declaring := cellsDeclaring(repository, tag)
	c.Assert(len(declaring) > 0, qt.IsTrue,
		qt.Commentf("this repository starts %s and no matrix cell declares it, so nothing here can "+
			"describe the capabilities of the server it runs (add a cell in cells.go)", ref))
	for _, cell := range declaring {
		c.Check(cell.Dialect, qt.Equals, dialect,
			qt.Commentf("cell %s declares image %q, whose repository speaks %s", cell, cell.Image, dialect))
	}
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

// integrationWorkflow is the CI job that starts database services directly.
//
// docker-compose.yaml is deliberately NOT read here even though
// pinnedImageFiles above reads both. Compose is the local convenience `make
// integration-test` drives; nothing in continuous integration starts it. A
// support level that counted it would let a line claim certification on the
// strength of a container only a developer's laptop runs, which is the
// difference between "Ptah tests this" and "Ptah could test this".
const integrationWorkflow = "../../.github/workflows/go-integration-tests.yml"

// TestCells_DeclareAValidSupportLevel is the census. A cell added without a
// level would otherwise carry the zero value, and the zero value renders as an
// empty column rather than as the omission it is.
func TestCells_DeclareAValidSupportLevel(t *testing.T) {
	c := qt.New(t)

	c.Assert(len(capabilityprobe.Cells) > 0, qt.IsTrue)
	for _, cell := range capabilityprobe.Cells {
		t.Run(capabilityprobe.CellID(cell), func(t *testing.T) {
			c := qt.New(t)
			c.Assert(cell.Support.Valid(), qt.IsTrue,
				qt.Commentf("cell %s declares support level %q, which capability.SupportLevel does not define",
					cell, cell.Support))
		})
	}
}

// TestCells_CertificationMatchesWhatContinuousIntegrationRuns is what makes the
// level a claim rather than a label.
//
// Certified and legacy-tested both assert that Ptah regularly exercises the
// line. Three mechanisms do that — the capability probe the tiered workflows
// fan out over, the integration suite's own databases, and the engine compiled
// into the binary under test — and a line outside all three is measured by
// nothing, whatever its vendor says about it. Reading the answer out of the
// matrix and the workflow means a cell cannot claim certification by being
// written down.
func TestCells_CertificationMatchesWhatContinuousIntegrationRuns(t *testing.T) {
	c := qt.New(t)

	exercised := exercisedLines(c)
	c.Assert(len(exercised) > 0, qt.IsTrue,
		qt.Commentf("no line was found to be exercised at all, so every assertion below would be vacuous"))

	for _, cell := range capabilityprobe.Cells {
		t.Run(capabilityprobe.CellID(cell), func(t *testing.T) {
			c := qt.New(t)
			claimsTesting := cell.Support == capability.Certified || cell.Support == capability.LegacyTested
			// An emulated line is the one place the two come apart. Running an
			// emulator on every pull request catches a preset drifting from the
			// interface, and says nothing about the managed service, so the
			// line is exercised and stays best-effort. Anything else exercised
			// must claim the level, and anything unexercised must not.
			shouldClaim := exercised[capabilityprobe.CellID(cell)] && !cell.Emulated
			c.Assert(claimsTesting, qt.Equals, shouldClaim,
				qt.Commentf("cell %s declares %q; continuous integration exercises it: %v; emulated: %v",
					cell, cell.Support, exercised[capabilityprobe.CellID(cell)], cell.Emulated))
		})
	}
}

// TestCells_BestEffortLinesAreExactlyTheUnmeasuredOnes pins the five today, so
// that a line quietly losing its coverage shows up as this list growing rather
// than as a level nobody re-read. The previous test proves the rule; this one
// records the current answer, which is the part a reader of the support matrix
// is actually asking about.
func TestCells_BestEffortLinesAreExactlyTheUnmeasuredOnes(t *testing.T) {
	c := qt.New(t)

	unmeasured := slices.DeleteFunc(slices.Clone(capabilityprobe.Cells), func(cell capabilityprobe.Cell) bool {
		return cell.Support != capability.BestEffort
	})
	bestEffort := make([]string, 0, len(unmeasured))
	for _, cell := range unmeasured {
		bestEffort = append(bestEffort, capabilityprobe.CellID(cell))
	}

	// ClickHouse 26.3 and 25.8 left this list in stokaro/ptah#916: the matrix
	// gained a ClickHouse launch recipe, so both lines have a probe job now and
	// a line CI exercises may not call itself best-effort.
	c.Assert(bestEffort, qt.ContentEquals, []string{
		"sqlserver-16-0",
		"sqlserver-15-0",
		"spanner-0",
	})
}

// TestCells_DeclareNoKnownIncompatibleLine records a fact rather than a rule.
// The level exists because the vocabulary needs it; no line carries it because
// no concrete technical incompatibility has been found. If one ever is, this
// test is the place the change announces itself.
func TestCells_DeclareNoKnownIncompatibleLine(t *testing.T) {
	c := qt.New(t)

	for _, cell := range capabilityprobe.Cells {
		c.Assert(cell.Support, qt.Not(qt.Equals), capability.KnownIncompatible)
	}
}

// exercisedLines answers, per cell id, whether continuous integration runs
// anything against that release line.
func exercisedLines(c *qt.C) map[string]bool {
	c.Helper()

	exercised := make(map[string]bool, len(capabilityprobe.Cells))
	for _, cell := range capabilityprobe.Cells {
		exercised[capabilityprobe.CellID(cell)] = false
	}

	matrix := capabilityprobe.CIMatrix()
	c.Assert(len(matrix.Cells) > 0, qt.IsTrue,
		qt.Commentf("the capability matrix reported no runnable cell, so this check would examine nothing"))
	for _, ci := range matrix.Cells {
		exercised[ci.ID] = true
	}

	for _, cell := range suiteStartedCells(c) {
		exercised[capabilityprobe.CellID(cell)] = true
	}

	// SQLite has no container anywhere and needs none: the engine is compiled
	// into the binary under test, so `go test ./...` exercises this line on
	// every run of every job. Naming it here rather than exempting it keeps
	// the rule above total.
	for _, cell := range capabilityprobe.Cells {
		if cell.Dialect == platform.SQLite {
			exercised[capabilityprobe.CellID(cell)] = true
		}
	}
	return exercised
}

// suiteStartedCells returns the cells whose line the integration workflow
// starts a server for, whether it declares the container as a job service or
// starts it from a step with `docker run`. Both count: the job runs against
// either one, and CockroachDB and YugabyteDB are only reachable the second way.
func suiteStartedCells(c *qt.C) []capabilityprobe.Cell {
	c.Helper()

	var out []capabilityprobe.Cell
	for _, ref := range startedImages(c, integrationWorkflow) {
		out = append(out, cellsDeclaring(splitImageRef(ref))...)
	}
	c.Assert(len(out) > 0, qt.IsTrue,
		qt.Commentf("%s started no image any cell declares, which would silently drop every "+
			"suite-only line to best-effort", integrationWorkflow))
	return out
}

// renovateConfig is the dependency-automation policy. A database server image
// carries a release line rather than a version number, so its updates are
// classified by hand; the rule that says so names the image repositories, and
// this file is where that name list is checked against the declaration.
const renovateConfig = "../../renovate.json"

// databaseImagesRuleGroup is the groupName of the rule under test. Matching on
// it rather than on position means a rule reordered above stays found.
const databaseImagesRuleGroup = "database server images"

// TestRenovate_ClassifiesEveryDatabaseServerImage keeps a hand-written list
// honest.
//
// renovate.json cannot read Go, so the image repositories it must not
// auto-merge are written out there — and a written-out list is a claim that
// was true when it was typed. A new engine added to cells.go with no entry in
// that rule would get ordinary dependency treatment: its release lines would
// be replaced by a bot rather than classified, which is the exact failure the
// rule exists to prevent.
func TestRenovate_ClassifiesEveryDatabaseServerImage(t *testing.T) {
	c := qt.New(t)

	rule := databaseImageRule(c)
	c.Assert(rule.AutoMerge, qt.IsNotNil,
		qt.Commentf("the rule must state automerge explicitly; inheriting it is how a server image "+
			"comes to be merged by a bot"))
	c.Assert(*rule.AutoMerge, qt.IsFalse)

	declared := declaredImageRepositories()
	c.Assert(len(declared) > 0, qt.IsTrue,
		qt.Commentf("no cell declares a container image, so every assertion below would be vacuous"))

	for _, repository := range declared {
		t.Run(repository, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(rule.MatchPackageNames, qt.Contains, repository,
				qt.Commentf("cells.go declares an image from %q, and %s does not classify it: a Renovate "+
					"pull request replacing that line's tag would be treated as an ordinary bump",
					repository, renovateConfig))
		})
	}

	// The other direction: a repository named in the rule that no cell
	// declares is a rule nobody exercises, and it hides the removal of the
	// engine it was written for.
	for _, repository := range rule.MatchPackageNames {
		t.Run("declared "+repository, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(declared, qt.Contains, repository)
		})
	}
}

type renovatePackageRule struct {
	GroupName         string   `json:"groupName"`
	MatchPackageNames []string `json:"matchPackageNames"`
	AutoMerge         *bool    `json:"automerge"`
}

func databaseImageRule(c *qt.C) renovatePackageRule {
	c.Helper()

	body, err := os.ReadFile(renovateConfig)
	c.Assert(err, qt.IsNil)

	var config struct {
		PackageRules []renovatePackageRule `json:"packageRules"`
	}
	c.Assert(json.Unmarshal(body, &config), qt.IsNil)
	c.Assert(len(config.PackageRules) > 0, qt.IsTrue)

	matching := slices.DeleteFunc(slices.Clone(config.PackageRules), func(rule renovatePackageRule) bool {
		return rule.GroupName != databaseImagesRuleGroup
	})
	c.Assert(matching, qt.HasLen, 1,
		qt.Commentf("%s must carry exactly one %q rule", renovateConfig, databaseImagesRuleGroup))
	return matching[0]
}

// declaredImageRepositories returns the image repositories the cells declare,
// with tags removed and duplicates collapsed.
func declaredImageRepositories() []string {
	var out []string
	for _, cell := range capabilityprobe.Cells {
		repository, _ := splitImageRef(cell.Image)
		out = append(out, repository)
	}
	out = slices.DeleteFunc(out, func(repository string) bool { return repository == "" })
	return slices.Compact(slices.Sorted(slices.Values(out)))
}

// A versionless line accepts any version, and it has to: over the PostgreSQL
// wire a live Spanner endpoint announces `PostgreSQL 14.1`, which is
// PGAdapter's compatibility level rather than anything about Spanner. Matching
// a line against it would pin the cell to a number about a different product
// and move the day that number changed.
//
// Measured against a live endpoint: before this, the probe reported
// `no matrix cell covers spanner 14.1 (add a cell in cells.go)` while the cell
// was sitting in cells.go (stokaro/ptah#942).
func TestCellMatch_VersionlessLineAcceptsAnyVersion(t *testing.T) {
	tests := []struct {
		name    string
		version string
	}{
		{name: "the compatibility level a live endpoint announces", version: "PostgreSQL 14.1"},
		{name: "a different one, should PGAdapter raise it", version: "PostgreSQL 17.4"},
		{name: "a bare major", version: "PostgreSQL 15"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			version, err := capabilityprobe.ParseVersion(platform.Spanner, tt.version, "")
			c.Assert(err, qt.IsNil)

			cell := capabilityprobe.Cell{Dialect: platform.Spanner, Line: "0", Versionless: true}

			c.Assert(cell.Match(version), qt.IsTrue)
		})
	}
}

// The control: the flag is what makes the match unconditional, not the shape of
// the line. The same cell without it compares components and refuses.
func TestCellMatch_AVersionedLineStillComparesComponents(t *testing.T) {
	tests := []struct {
		name    string
		line    string
		version string
		want    bool
	}{
		{name: "a major accepts its patches", line: "17", version: "PostgreSQL 17.4", want: true},
		{name: "a major refuses another major", line: "17", version: "PostgreSQL 16.9", want: false},
		{name: "the spanner line without the flag refuses", line: "0", version: "PostgreSQL 14.1", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			version, err := capabilityprobe.ParseVersion(platform.Spanner, tt.version, "")
			c.Assert(err, qt.IsNil)

			cell := capabilityprobe.Cell{Dialect: platform.Spanner, Line: tt.line}

			c.Assert(cell.Match(version), qt.Equals, tt.want)
		})
	}
}

// A versionless cell answers for every server of its dialect, so a second cell
// on that dialect would be unreachable -- CellFor returns the first match and
// the versionless one matches everything. It is also the invariant the probe's
// line attribution rests on: an observation is credited to a versionless line
// because there is no sibling release to confuse it with.
func TestCells_AVersionlessDialectDeclaresExactlyOneLine(t *testing.T) {
	c := qt.New(t)

	// Counted through a map literal rather than a condition: the invariant is
	// arithmetic, and the assertion below reads it as arithmetic too.
	countOf := map[bool]int{true: 1, false: 0}
	lines := make(map[string]int)
	versionless := make(map[string]int)
	for _, cell := range capabilityprobe.Cells {
		lines[cell.Dialect]++
		versionless[cell.Dialect] += countOf[cell.Versionless]
	}

	for dialect, declared := range lines {
		// Either the dialect declares no versionless line, or it declares
		// exactly one line in total. Both are the same equation.
		c.Assert(versionless[dialect]*(declared-1), qt.Equals, 0,
			qt.Commentf("%s declares %d versionless line(s) among %d lines; a versionless line matches "+
				"every version, so a sibling line would be unreachable",
				dialect, versionless[dialect], declared))
	}

	total := 0
	for _, n := range versionless {
		total += n
	}
	c.Assert(total, qt.Equals, 1,
		qt.Commentf("a new versionless dialect needs the attribution reasoning re-read, not just the flag"))
	c.Assert(versionless[platform.Spanner], qt.Equals, 1)
}

// A declared understatement is a claim about a decision, so it has to carry the
// decision. Two ways it goes stale, both build failures rather than silent
// weakenings of the probe:
//
// A reason nobody wrote makes the declaration indistinguishable from a bug
// somebody silenced. And a declaration for a key the preset already claims is
// finished work still standing -- it would sit there licensing an
// understatement that no longer exists, ready to hide the next real one.
func TestCells_EveryDeclaredUnderstatementIsStillOne(t *testing.T) {
	type declaration struct {
		cell   capabilityprobe.Cell
		key    capability.Capability
		reason string
	}

	var declarations []declaration
	for _, cell := range capabilityprobe.Cells {
		for key, reason := range cell.Understates {
			declarations = append(declarations, declaration{cell: cell, key: key, reason: reason})
		}
	}

	c := qt.New(t)
	c.Assert(declarations, qt.Not(qt.HasLen), 0,
		qt.Commentf("this test is the only thing holding the mechanism honest; it must not pass vacuously"))

	for _, d := range declarations {
		t.Run(d.cell.Dialect+" "+string(d.key), func(t *testing.T) {
			c := qt.New(t)

			c.Assert(strings.TrimSpace(d.reason), qt.Not(qt.Equals), "",
				qt.Commentf("an understatement with no reason cannot be told from a silenced defect"))
			c.Assert(d.cell.Measured(), qt.IsTrue,
				qt.Commentf("a cell with no preset has nothing to understate"))
			c.Assert(d.cell.Preset().Has(d.key), qt.IsFalse,
				qt.Commentf("%s already claims %s, so this declaration is stale and would hide the next real difference",
					d.cell.PresetName, d.key))
		})
	}
}
