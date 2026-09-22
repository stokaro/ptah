// Package eolcheck asks endoflife.date whether a declared release line is
// still supported by its vendor, and reports the lines whose declared support
// level promises more than the vendor's calendar allows.
//
// It sits beside internal/capabilityprobe rather than inside it because the
// two answer different questions. A cell's Support level describes THIS
// repository's testing, and says nothing about the vendor. End of life is the
// other half: it moves on the vendor's calendar rather than on a commit, so a
// line whose declaration overtakes its upstream support is not something a
// build can notice.
//
// Nothing here removes a release line, and nothing here is a reason to refuse
// a server. [capability.SupportLevel] states that contract: upstream end of
// life lowers the testing guarantee Ptah offers and nothing else, and
// KnownIncompatible needs a named technical incompatibility rather than a
// date. So the outcome of a finding is a lower promise --
// [capability.Certified] gives way to [capability.LegacyTested], which is the
// level for a line kept past upstream end of life -- while the preset, the
// probe job and the dialect stay exactly where they are.
package eolcheck

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/capabilityprobe"
)

// Product is the endoflife.date identifier for one engine.
//
// The mapping is written out because it is not derivable: endoflife.date names
// SQL Server `mssqlserver` and Oracle `oracle-database`, and a dialect with no
// entry there has no row to read rather than an empty one.
var Product = map[string]string{
	platform.Postgres:    "postgresql",
	platform.MySQL:       "mysql",
	platform.MariaDB:     "mariadb",
	platform.SQLServer:   "mssqlserver",
	platform.Oracle:      "oracle-database",
	platform.CockroachDB: "cockroachdb",
	platform.ClickHouse:  "clickhouse",
}

// Unlisted names the dialects endoflife.date does not carry, so a run says
// which lines it could not ask about instead of reporting them supported.
//
// YugabyteDB has no entry; SQLite publishes no end-of-life calendar at all and
// is compiled into the binary rather than run as a server; Spanner is a
// managed service whose only container is an emulator with no release line.
var Unlisted = map[string]string{
	platform.YugabyteDB: "endoflife.date carries no yugabytedb product",
	platform.SQLite:     "SQLite publishes no end-of-life calendar, and the engine is compiled in",
	platform.Spanner:    "a managed service; the cell names an emulator rather than a release line",
}

// Cycle is one release line as endoflife.date reports it.
type Cycle struct {
	// Cycle is the line's identifier there, which is the string a cell's Line
	// has to equal for the two to be talking about the same release.
	Cycle string
	// EOL is the end-of-life date, or the zero time when the product reports a
	// boolean instead of a date.
	EOL time.Time
	// Ended is what a boolean `eol` said, and is what EOL cannot express.
	Ended bool
	// HasDate says whether EOL carries a date.
	HasDate bool
}

// Ceased reports whether the line had reached end of life by on.
func (c Cycle) Ceased(on time.Time) bool {
	if !c.HasDate {
		return c.Ended
	}
	return c.EOL.Before(on)
}

// Finding is one declared line the vendor no longer supports.
type Finding struct {
	// Cell is the declared line.
	Cell capabilityprobe.Cell
	// Product is the endoflife.date identifier the answer came from.
	Product string
	// EOL is when support ended, or the zero time when the product reported a
	// boolean rather than a date.
	EOL time.Time
	// HasDate says whether EOL carries a date.
	HasDate bool
}

// Overstated reports whether the cell promises more than an end-of-life line
// can carry.
//
// [capability.Certified] is the only level that does. LegacyTested is the
// level a line kept past upstream end of life takes, BestEffort declares no
// testing at all, and KnownIncompatible is reserved for a named technical
// incompatibility. So a line already declared at one of those three is
// correctly declared, and appears in a report without asking for a change.
func (f Finding) Overstated() bool {
	return f.Cell.Support == capability.Certified
}

// ID is the identifier a branch, a pull request and a skip label share. It is
// the cell's own id, so one line has one name everywhere.
func (f Finding) ID() string {
	return capabilityprobe.CellID(f.Cell)
}

// Ended renders the end of support for a reader.
func (f Finding) Ended() string {
	if !f.HasDate {
		return "reported without a date"
	}
	return f.EOL.Format("2006-01-02")
}

// UnprobedReason is the sentence a demoted cell carries in its Unprobed field.
//
// It lives here rather than in the workflow that spends it, because it is what
// a reader of the support matrix is given for the line no longer running, and
// a sentence assembled in YAML is a sentence nothing reads back.
func (f Finding) UnprobedReason() string {
	when := "with no date published"
	if f.HasDate {
		when = "on " + f.EOL.Format("2006-01-02")
	}
	return fmt.Sprintf("upstream support ended %s (endoflife.date/%s)", when, f.Product)
}

// Unanswered is one declared line no vendor calendar covers.
type Unanswered struct {
	// Cell is the declared line.
	Cell capabilityprobe.Cell
	// Reason says why nothing could be asked.
	Reason string
}

// Report is what one run found.
type Report struct {
	// On is the date the run compared against.
	On time.Time
	// Findings are the lines past end of life, ordered by cell id. A line is
	// listed whatever it declares; [Finding.Overstated] separates the ones
	// whose declaration has to change from the ones already lowered.
	Findings []Finding
	// Unanswered are the lines no calendar covers, ordered by cell id.
	Unanswered []Unanswered
	// Asked counts the lines a calendar answered, which is what keeps an
	// empty Findings from reading as success on a run that asked nothing.
	Asked int
}

// Fetcher reads one product's cycles.
type Fetcher func(ctx context.Context, product string) ([]Cycle, error)

// Check compares every declared cell against the vendor calendars.
//
// A cell whose line the product does not list is neither a finding nor an
// unanswered line: it is a line this repository names differently from the
// vendor, and saying "supported" about it would be the same invention as
// saying "ended".
func Check(ctx context.Context, cells []capabilityprobe.Cell, on time.Time, fetch Fetcher) (Report, error) {
	report := Report{On: on}
	cycles := make(map[string][]Cycle)
	for _, cell := range cells {
		product, listed := Product[platform.NormalizeDialect(cell.Dialect)]
		if !listed {
			report.Unanswered = append(report.Unanswered, Unanswered{
				Cell:   cell,
				Reason: Unlisted[platform.NormalizeDialect(cell.Dialect)],
			})
			continue
		}
		if _, read := cycles[product]; !read {
			got, err := fetch(ctx, product)
			if err != nil {
				return Report{}, fmt.Errorf("read the %s calendar: %w", product, err)
			}
			cycles[product] = got
		}
		cycle, found := findCycle(cycles[product], cell.Line)
		if !found {
			report.Unanswered = append(report.Unanswered, Unanswered{
				Cell:   cell,
				Reason: fmt.Sprintf("%s lists no cycle %q", product, cell.Line),
			})
			continue
		}
		report.Asked++
		if !cycle.Ceased(on) {
			continue
		}
		report.Findings = append(report.Findings, Finding{
			Cell: cell, Product: product, EOL: cycle.EOL, HasDate: cycle.HasDate,
		})
	}
	sort.Slice(report.Findings, func(i, j int) bool {
		return report.Findings[i].ID() < report.Findings[j].ID()
	})
	sort.Slice(report.Unanswered, func(i, j int) bool {
		return capabilityprobe.CellID(report.Unanswered[i].Cell) <
			capabilityprobe.CellID(report.Unanswered[j].Cell)
	})
	return report, nil
}

func findCycle(cycles []Cycle, line string) (Cycle, bool) {
	for _, cycle := range cycles {
		if cycle.Cycle == line {
			return cycle, true
		}
	}
	return Cycle{}, false
}

// Endpoint is the address one product's calendar is read from.
func Endpoint(product string) string {
	return "https://endoflife.date/api/" + product + ".json"
}

// HTTPFetcher reads a product's cycles over HTTP.
func HTTPFetcher(client *http.Client) Fetcher {
	return func(ctx context.Context, product string) ([]Cycle, error) {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, Endpoint(product), nil)
		if err != nil {
			return nil, err
		}
		response, err := client.Do(request)
		if err != nil {
			return nil, err
		}
		defer func() { _ = response.Body.Close() }()
		if response.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("%s answered %s", Endpoint(product), response.Status)
		}
		return DecodeCycles(response.Body)
	}
}

// DecodeCycles reads the array endoflife.date publishes.
//
// The `eol` field is a date, a boolean, or absent, and the three mean
// different things: a date says when support ends, `true` says it has ended
// without saying when, and `false` says it has not. Decoding into one Go type
// would make the second and third indistinguishable from the zero value.
func DecodeCycles(r io.Reader) ([]Cycle, error) {
	var raw []struct {
		Cycle any `json:"cycle"`
		EOL   any `json:"eol"`
	}
	if err := json.NewDecoder(r).Decode(&raw); err != nil {
		return nil, err
	}
	cycles := make([]Cycle, 0, len(raw))
	for _, row := range raw {
		cycle := Cycle{Cycle: fmt.Sprintf("%v", row.Cycle)}
		switch value := row.EOL.(type) {
		case bool:
			cycle.Ended = value
		case string:
			parsed, err := time.Parse("2006-01-02", strings.TrimSpace(value))
			if err != nil {
				return nil, fmt.Errorf("cycle %q has an unreadable eol %q: %w", cycle.Cycle, value, err)
			}
			cycle.EOL, cycle.HasDate = parsed, true
		}
		cycles = append(cycles, cycle)
	}
	return cycles, nil
}
