// Package shard splits Ptah's tagged integration contour across parallel
// runners and checks that the runners' reports cover it exactly once.
//
// Plan is the partition each runner computes for itself from the listed tests,
// an optional timing report from an earlier run and the runner count. It is a
// pure function of those inputs, so every runner that reads the same inputs
// computes the same partition, and its digest lets a reader prove they did.
// Verify is the other half: it reads every runner's manifest and go test -json
// report and refuses a contour in which a listed test is missing, ran more
// than once, skipped or failed.
//
// The timings change only how the tests are balanced across runners. Which
// tests run is decided by the listing alone, and Verify judges the reports
// against that listing, never against the timings.
package shard

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strings"
)

// countWeightMS is the weight of a test no timing measured when the report
// measured no test at all, so the partition balances by test count.
const countWeightMS = 1000

// Test names one top-level test function of one package.
type Test struct {
	Package string `json:"package"`
	Name    string `json:"name"`
}

// String returns the test as package:name.
func (t Test) String() string {
	return t.Package + ":" + t.Name
}

func compareTests(left, right Test) int {
	return cmp.Or(cmp.Compare(left.Package, right.Package), cmp.Compare(left.Name, right.Name))
}

// Timings is what an earlier run of the contour measured, read to balance the
// next partition. The zero value measures nothing, and Plan then gives every
// test the same weight.
type Timings struct {
	// Source says where the measurement came from. Plan copies it into the
	// partition's Weighting so a run log names the run it was balanced on.
	Source string `json:"source,omitempty"`
	// Packages maps an import path to its measurement.
	Packages map[string]PackageTiming `json:"packages,omitempty"`
}

// PackageTiming is one package's measurement, in milliseconds.
type PackageTiming struct {
	// ElapsedMS is the package's whole run: every test plus what the test
	// binary spends outside them, such as TestMain. A package split across
	// several shards pays that outside cost on each of them, and it is counted
	// once, at the most one shard spent.
	ElapsedMS int64 `json:"elapsed_ms"`
	// Tests maps a top-level test name to its elapsed time.
	Tests map[string]int64 `json:"tests_ms,omitempty"`
}

// ReadTimings decodes a timing report. Unknown fields are refused, so a report
// written in another shape is reported rather than read as an empty one.
func ReadTimings(r io.Reader) (Timings, error) {
	decoder := json.NewDecoder(r)
	decoder.DisallowUnknownFields()
	var timings Timings
	if err := decoder.Decode(&timings); err != nil {
		return Timings{}, fmt.Errorf("decode contour timings: %w", err)
	}
	return timings, nil
}

// Unit is one package and the tests of it that a part runs.
type Unit struct {
	Package string `json:"package"`
	// Whole reports that the package was assigned as a whole, so the part
	// runs it without a -run filter. A package too heavy for one part is
	// split by test name instead, and each part holding some of its tests
	// carries a Unit with Whole unset.
	Whole bool `json:"whole"`
	// Tests lists every test of the package the part runs, sorted.
	Tests []string `json:"tests"`
}

// Part is what one shard runs.
type Part struct {
	// Units are sorted by package.
	Units []Unit `json:"units"`
	// WeightMS is the planned duration: the sum of the weights assigned to
	// the part.
	WeightMS int64 `json:"weight_ms"`
}

// Invocations returns the package and -run arguments of the go test runs
// that execute the part, in the order the unsharded contour runs the same
// tests. Two tests that share a part then run in the order they run in when
// one go test run executes the whole contour, so a test that leaves state
// behind meets no test it did not meet there.
//
// The unsharded run goes through packages in import-path order and runs a
// package's tests in the order go test -list prints them; listed is that
// listing. Consecutive whole packages share one run without a filter. Each
// package split by test name gets runs of its own whose -run patterns follow
// the listing order and are each at most limit bytes, so that no argument
// outgrows what an operating system passes to a process; a single name longer
// than limit still gets a pattern of its own. A test listed nowhere in listed
// runs after the package's listed tests, in name order.
func (p Part) Invocations(listed []Test, limit int) [][]string {
	position := make(map[Test]int, len(listed))
	for index, test := range listed {
		position[test] = index
	}
	var invocations [][]string
	var whole []string
	for _, unit := range p.Units {
		if unit.Whole {
			whole = append(whole, unit.Package)
			continue
		}
		if len(whole) != 0 {
			invocations = append(invocations, whole)
			whole = nil
		}
		rank := func(name string) int {
			if index, ok := position[Test{Package: unit.Package, Name: name}]; ok {
				return index
			}
			return len(listed)
		}
		// Tests are sorted by name, so the stable sort leaves the unlisted
		// ones, which share the last rank, in name order.
		names := slices.Clone(unit.Tests)
		slices.SortStableFunc(names, func(left, right string) int {
			return cmp.Compare(rank(left), rank(right))
		})
		for _, pattern := range runPatterns(names, limit) {
			invocations = append(invocations, []string{"-run", pattern, unit.Package})
		}
	}
	if len(whole) != 0 {
		invocations = append(invocations, whole)
	}
	return invocations
}

// runPatterns returns the anchored alternations that select exactly names, in
// their order, each at most limit bytes long where a name allows it.
func runPatterns(names []string, limit int) []string {
	if len(names) == 0 {
		return nil
	}
	const prefix, suffix = "^(?:", ")$"
	var patterns []string
	var current []string
	size := len(prefix) + len(suffix)
	for _, name := range names {
		quoted := regexp.QuoteMeta(name)
		if len(current) != 0 && size+1+len(quoted) > limit {
			patterns = append(patterns, prefix+strings.Join(current, "|")+suffix)
			current = nil
			size = len(prefix) + len(suffix)
		}
		if len(current) != 0 {
			size++
		}
		current = append(current, quoted)
		size += len(quoted)
	}
	return append(patterns, prefix+strings.Join(current, "|")+suffix)
}

// Tests returns every test the part runs, sorted.
func (p Part) Tests() []Test {
	var tests []Test
	for _, unit := range p.Units {
		for _, name := range unit.Tests {
			tests = append(tests, Test{Package: unit.Package, Name: name})
		}
	}
	slices.SortFunc(tests, compareTests)
	return tests
}

// Partition assigns every listed test to exactly one part.
type Partition struct {
	// Tests is the whole contour as listed, sorted.
	Tests []Test `json:"tests"`
	// Parts holds one entry per shard, in shard order.
	Parts []Part `json:"parts"`
	// Weighting says what balanced the parts: the timing report's source, or
	// the test count when no test was measured.
	Weighting string `json:"weighting"`
	// Digest is a hash of the fields above. Two runners that computed the
	// same partition report the same digest.
	Digest string `json:"digest"`
}

// ComputeDigest returns the digest of the partition's tests, parts and
// weighting. Verify compares it with Digest to detect a manifest that does not
// hold the partition it claims.
func (p Partition) ComputeDigest() string {
	payload, err := json.Marshal(struct {
		Tests     []Test `json:"tests"`
		Parts     []Part `json:"parts"`
		Weighting string `json:"weighting"`
	}{p.Tests, p.Parts, p.Weighting})
	if err != nil {
		// The payload holds only strings, integers and slices of them.
		panic(fmt.Sprintf("encode contour partition: %v", err))
	}
	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// ErrNoTests reports a partition asked for over an empty listing.
var ErrNoTests = errors.New("the contour lists no tests")

// item is one unit of assignment: a whole package, or one test of a package
// too heavy for a single part.
type item struct {
	pkg    string
	test   string // empty for a whole package
	tests  []string
	weight int64
}

// Plan partitions tests across shards parts.
//
// A package is assigned whole unless it weighs more than an equal share of the
// contour, in which case its tests are assigned one by one. The units are then
// placed heaviest first, each on the lightest part so far, with ties broken by
// package, test name and part index; the order of tests in the argument does
// not matter. Every test lands in exactly one part, and a part that would hold
// nothing is refused rather than returned.
//
// A test's weight is its elapsed time in timings. A test timings does not
// measure weighs the median of the measured ones, and when timings measures no
// listed test every test weighs the same, so the partition balances by count.
func Plan(tests []Test, timings Timings, shards int) (Partition, error) {
	if shards < 1 {
		return Partition{}, fmt.Errorf("shard count must be positive, got %d", shards)
	}
	if len(tests) == 0 {
		return Partition{}, ErrNoTests
	}
	sorted := slices.Clone(tests)
	slices.SortFunc(sorted, compareTests)
	for index, test := range sorted {
		if test.Package == "" || test.Name == "" {
			return Partition{}, fmt.Errorf("listed test %q has an empty package or name", test.String())
		}
		if index > 0 && sorted[index-1] == test {
			return Partition{}, fmt.Errorf("test %s is listed twice", test)
		}
	}

	weights := newWeigher(sorted, timings)
	items := weights.items(sorted, shards)
	slices.SortFunc(items, func(left, right item) int {
		return cmp.Or(
			cmp.Compare(right.weight, left.weight),
			cmp.Compare(left.pkg, right.pkg),
			cmp.Compare(left.test, right.test),
		)
	})
	if len(items) < shards {
		return Partition{}, fmt.Errorf(
			"%d shards exceed the %d units the contour can be split into", shards, len(items),
		)
	}

	assigned := make([][]item, shards)
	loads := make([]int64, shards)
	for _, next := range items {
		lightest := 0
		for index := 1; index < shards; index++ {
			if loads[index] < loads[lightest] {
				lightest = index
			}
		}
		assigned[lightest] = append(assigned[lightest], next)
		loads[lightest] += next.weight
	}

	partition := Partition{Tests: sorted, Weighting: weights.description}
	for index, parts := range assigned {
		partition.Parts = append(partition.Parts, Part{Units: units(parts), WeightMS: loads[index]})
	}
	partition.Digest = partition.ComputeDigest()
	return partition, nil
}

func units(items []item) []Unit {
	byPackage := make(map[string]*Unit)
	for _, next := range items {
		unit, ok := byPackage[next.pkg]
		if !ok {
			unit = &Unit{Package: next.pkg, Whole: next.test == ""}
			byPackage[next.pkg] = unit
		}
		unit.Tests = append(unit.Tests, next.tests...)
	}
	out := make([]Unit, 0, len(byPackage))
	for _, unit := range byPackage {
		slices.Sort(unit.Tests)
		out = append(out, *unit)
	}
	slices.SortFunc(out, func(left, right Unit) int { return cmp.Compare(left.Package, right.Package) })
	return out
}

// weigher holds the weight of every listed test and package.
type weigher struct {
	timings     Timings
	fallback    int64
	description string
}

func newWeigher(tests []Test, timings Timings) weigher {
	var measured []int64
	for _, test := range tests {
		if elapsed, ok := timings.Packages[test.Package].Tests[test.Name]; ok {
			measured = append(measured, max(elapsed, 1))
		}
	}
	if len(measured) == 0 {
		description := "test count"
		switch {
		case timings.Source != "" && len(timings.Packages) == 0:
			description += " (" + timings.Source + ")"
		case timings.Source != "":
			description += " (" + timings.Source + " measured none of the listed tests)"
		}
		return weigher{fallback: countWeightMS, description: description}
	}
	slices.Sort(measured)
	return weigher{
		timings:  timings,
		fallback: measured[len(measured)/2],
		description: fmt.Sprintf(
			"timings from %s (%d of %d listed tests measured)",
			cmp.Or(timings.Source, "an unnamed report"), len(measured), len(tests),
		),
	}
}

func (w weigher) test(test Test) int64 {
	if elapsed, ok := w.timings.Packages[test.Package].Tests[test.Name]; ok {
		return max(elapsed, 1)
	}
	return w.fallback
}

// pkg weighs a package as the sum of its listed tests plus what its test binary
// spent outside them in the measured run.
func (w weigher) pkg(name string, tests []string) int64 {
	var total int64
	for _, test := range tests {
		total += w.test(Test{Package: name, Name: test})
	}
	measured, ok := w.timings.Packages[name]
	if !ok {
		return total
	}
	var inside int64
	for _, elapsed := range measured.Tests {
		inside += elapsed
	}
	return total + max(measured.ElapsedMS-inside, 0)
}

func (w weigher) items(sorted []Test, shards int) []item {
	var packages []string
	byPackage := make(map[string][]string)
	for _, test := range sorted {
		if _, ok := byPackage[test.Package]; !ok {
			packages = append(packages, test.Package)
		}
		byPackage[test.Package] = append(byPackage[test.Package], test.Name)
	}
	weights := make(map[string]int64, len(packages))
	var total int64
	for _, name := range packages {
		weights[name] = w.pkg(name, byPackage[name])
		total += weights[name]
	}
	share := (total + int64(shards) - 1) / int64(shards)

	var items []item
	for _, name := range packages {
		tests := byPackage[name]
		if weights[name] <= share || len(tests) == 1 {
			items = append(items, item{pkg: name, tests: tests, weight: weights[name]})
			continue
		}
		for _, test := range tests {
			items = append(items, item{
				pkg:    name,
				test:   test,
				tests:  []string{test},
				weight: w.test(Test{Package: name, Name: test}),
			})
		}
	}
	return items
}
