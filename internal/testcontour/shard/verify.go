package shard

import (
	"bufio"
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strconv"
	"strings"
)

// Manifest is what one shard records beside its report: which shard it is,
// and the partition it computed and ran its part of.
type Manifest struct {
	// Shard is the shard's position, counted from 1.
	Shard int `json:"shard"`
	// Shards is the number of shards the contour was split into.
	Shards    int       `json:"shards"`
	Partition Partition `json:"partition"`
}

// ReadManifest decodes a manifest, refusing unknown fields.
func ReadManifest(r io.Reader) (Manifest, error) {
	decoder := json.NewDecoder(r)
	decoder.DisallowUnknownFields()
	var manifest Manifest
	if err := decoder.Decode(&manifest); err != nil {
		return Manifest{}, fmt.Errorf("decode shard manifest: %w", err)
	}
	return manifest, nil
}

// Report is one shard's manifest and the go test -json stream it produced.
type Report struct {
	Manifest Manifest
	// Events is the shard's go test -json output. A shard that stopped before
	// writing any passes an empty reader, and its tests count as missing.
	Events io.Reader
}

// ShardResult is what one shard ran.
type ShardResult struct {
	Shard int
	// Tests is how many top-level tests the shard reported starting.
	Tests int
	// PlannedMS is the weight the partition gave the shard.
	PlannedMS int64
	// ElapsedMS is the sum of the elapsed time its test binaries reported.
	ElapsedMS int64
}

// Result is the gate's account of a sharded contour. Every slice is sorted.
type Result struct {
	// Shards is the number of parts the agreed partition has.
	Shards int
	// Weighting and Digest repeat the agreed partition's fields.
	Weighting string
	Digest    string
	// Listed is the size of the contour the shards partitioned.
	Listed int
	// Ran counts the listed tests that reported starting at least once.
	Ran int
	// Passed counts the listed tests that ran once and passed.
	Passed int

	// Absent lists the shards that sent no report. Their tests count as
	// Missing.
	Absent []int
	// Missing tests were listed and never reported starting.
	Missing []Test
	// Unexpected tests reported starting but were not listed.
	Unexpected []Test
	// Repeated tests reported starting more than once, on one shard or across
	// several.
	Repeated []Test
	// Misplaced tests ran on a shard other than the one the partition gave
	// them.
	Misplaced []Test
	// Incomplete tests reported starting but no pass, fail or skip.
	Incomplete []Test
	// Skipped holds every skipped test and subtest, as package:path.
	Skipped []string
	// Failed holds every failed top-level test, as package:name, and every
	// package whose test binary failed outside a test, as the import path.
	Failed []string

	PerShard []ShardResult
	// Timings is what the reports measured, for the next partition.
	Timings Timings
}

// Verify checks that the reports cover their partition exactly once. It
// refuses reports that disagree about the partition, a partition that lists
// fewer than floor tests, and any test that is missing, unexpected, repeated,
// misplaced, incomplete, skipped or failed. The Result is filled in either way,
// so a caller can print the account beside the error.
func Verify(reports []Report, floor int) (Result, error) {
	if floor < 1 {
		return Result{}, fmt.Errorf("the contour floor must be positive, got %d", floor)
	}
	partition, absent, err := agreedPartition(reports)
	if err != nil {
		return Result{}, err
	}

	result := Result{
		Shards:    len(partition.Parts),
		Weighting: partition.Weighting,
		Digest:    partition.Digest,
		Listed:    len(partition.Tests),
		Absent:    absent,
	}
	var failures []error
	if len(absent) != 0 {
		// The reports that arrived still agree on the partition, so the gate
		// judges and measures them; the absent shards' tests count as missing.
		failures = append(failures, fmt.Errorf(
			"shard %s of %d sent no report, so the tests given to it are unaccounted for",
			joinInts(absent), len(partition.Parts),
		))
	}
	if len(partition.Tests) < floor {
		failures = append(failures, fmt.Errorf(
			"the contour lists %d tests, below the floor of %d; a listing that stopped matching would look like this",
			len(partition.Tests), floor,
		))
	}

	listed := make(map[Test]int, len(partition.Tests))
	for index, part := range partition.Parts {
		for _, test := range part.Tests() {
			if owner, ok := listed[test]; ok {
				failures = append(failures, fmt.Errorf("the partition gives %s to shards %d and %d", test, owner, index+1))
				continue
			}
			listed[test] = index + 1
		}
	}
	for _, test := range partition.Tests {
		if _, ok := listed[test]; !ok {
			failures = append(failures, fmt.Errorf("the partition gives %s to no shard", test))
		}
	}

	ordered := slices.Clone(reports)
	slices.SortFunc(ordered, func(left, right Report) int {
		return cmp.Compare(left.Manifest.Shard, right.Manifest.Shard)
	})
	ledger := newLedger()
	for _, report := range ordered {
		shardResult, err := ledger.read(report)
		if err != nil {
			failures = append(failures, err)
		}
		shardResult.PlannedMS = partition.Parts[report.Manifest.Shard-1].WeightMS
		result.PerShard = append(result.PerShard, shardResult)
	}

	ledger.judge(listed, &result)
	result.Timings = ledger.timings()
	failures = append(failures, findings(result)...)
	return result, errors.Join(failures...)
}

// agreedPartition returns the partition every report carries and the shards
// that sent none, after checking that the reports carry the same partition and
// that no shard reported twice.
func agreedPartition(reports []Report) (Partition, []int, error) {
	if len(reports) == 0 {
		return Partition{}, nil, errors.New("no shard reported; nothing proves the contour ran")
	}
	first := reports[0].Manifest
	if first.Shards < 1 || len(first.Partition.Parts) != first.Shards {
		return Partition{}, nil, fmt.Errorf(
			"shard %d claims %d shards but carries a partition of %d parts",
			first.Shard, first.Shards, len(first.Partition.Parts),
		)
	}
	seen := make(map[int]bool, first.Shards)
	for _, report := range reports {
		manifest := report.Manifest
		if manifest.Shards != first.Shards {
			return Partition{}, nil, fmt.Errorf(
				"shard %d claims %d shards and shard %d claims %d",
				first.Shard, first.Shards, manifest.Shard, manifest.Shards,
			)
		}
		if manifest.Shard < 1 || manifest.Shard > manifest.Shards {
			return Partition{}, nil, fmt.Errorf("a report names shard %d of %d", manifest.Shard, manifest.Shards)
		}
		if seen[manifest.Shard] {
			return Partition{}, nil, fmt.Errorf("shard %d reported twice", manifest.Shard)
		}
		seen[manifest.Shard] = true
		if computed := manifest.Partition.ComputeDigest(); computed != manifest.Partition.Digest {
			return Partition{}, nil, fmt.Errorf(
				"shard %d carries a partition whose digest %s does not match its content (%s)",
				manifest.Shard, manifest.Partition.Digest, computed,
			)
		}
		if manifest.Partition.Digest != first.Partition.Digest {
			return Partition{}, nil, fmt.Errorf(
				"shards %d and %d computed different partitions (%s and %s); their inputs differed",
				first.Shard, manifest.Shard, first.Partition.Digest, manifest.Partition.Digest,
			)
		}
	}
	var absent []int
	for shard := 1; shard <= first.Shards; shard++ {
		if !seen[shard] {
			absent = append(absent, shard)
		}
	}
	return first.Partition, absent, nil
}

func joinInts(values []int) string {
	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, strconv.Itoa(value))
	}
	return strings.Join(parts, ", ")
}

type testEvent struct {
	Action  string
	Package string
	Test    string
	Elapsed float64
}

// ledger accumulates what every report says about every test.
type ledger struct {
	starts     map[Test][]int
	finished   map[Test]bool
	outcome    map[Test]string
	elapsed    map[Test]int64
	packageMS  map[string]int64
	skipped    map[string]bool
	failedPkgs map[string]bool
}

func newLedger() *ledger {
	return &ledger{
		starts:     make(map[Test][]int),
		finished:   make(map[Test]bool),
		outcome:    make(map[Test]string),
		elapsed:    make(map[Test]int64),
		packageMS:  make(map[string]int64),
		skipped:    make(map[string]bool),
		failedPkgs: make(map[string]bool),
	}
}

func (l *ledger) read(report Report) (ShardResult, error) {
	shard := report.Manifest.Shard
	result := ShardResult{Shard: shard}
	scanner := bufio.NewScanner(report.Events)
	scanner.Buffer(make([]byte, 0, 64<<10), 64<<20)
	line := 0
	for scanner.Scan() {
		line++
		if len(scanner.Bytes()) == 0 {
			continue
		}
		var event testEvent
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return result, fmt.Errorf("shard %d report line %d is not a test event: %w", shard, line, err)
		}
		l.record(shard, event, &result)
	}
	if err := scanner.Err(); err != nil {
		return result, fmt.Errorf("read shard %d report: %w", shard, err)
	}
	return result, nil
}

func (l *ledger) record(shard int, event testEvent, result *ShardResult) {
	elapsedMS := int64(event.Elapsed * 1000)
	if event.Test == "" {
		switch event.Action {
		case "pass":
			l.packageMS[event.Package] += elapsedMS
			result.ElapsedMS += elapsedMS
		case "fail":
			l.packageMS[event.Package] += elapsedMS
			result.ElapsedMS += elapsedMS
			l.failedPkgs[event.Package] = true
		}
		return
	}
	if event.Action == "skip" {
		l.skipped[event.Package+":"+event.Test] = true
	}
	if strings.Contains(event.Test, "/") {
		return
	}
	test := Test{Package: event.Package, Name: event.Test}
	switch event.Action {
	case "run":
		l.starts[test] = append(l.starts[test], shard)
		result.Tests++
	case "pass", "fail", "skip":
		l.finished[test] = true
		l.outcome[test] = event.Action
		l.elapsed[test] = elapsedMS
	}
}

func (l *ledger) judge(listed map[Test]int, result *Result) {
	for test, owner := range listed {
		starts := l.starts[test]
		if len(starts) == 0 {
			result.Missing = append(result.Missing, test)
			continue
		}
		result.Ran++
		if len(starts) > 1 {
			result.Repeated = append(result.Repeated, test)
		}
		if slices.ContainsFunc(starts, func(shard int) bool { return shard != owner }) {
			result.Misplaced = append(result.Misplaced, test)
		}
		if !l.finished[test] {
			result.Incomplete = append(result.Incomplete, test)
		}
		if l.outcome[test] == "pass" && len(starts) == 1 {
			result.Passed++
		}
	}
	for test := range l.starts {
		if _, ok := listed[test]; !ok {
			result.Unexpected = append(result.Unexpected, test)
		}
	}
	for test, outcome := range l.outcome {
		if outcome == "fail" {
			result.Failed = append(result.Failed, test.String())
		}
	}
	result.Failed = append(result.Failed, slices.Collect(maps.Keys(l.failedPkgs))...)
	result.Skipped = slices.Collect(maps.Keys(l.skipped))

	for _, tests := range []*[]Test{
		&result.Missing, &result.Unexpected, &result.Repeated, &result.Misplaced, &result.Incomplete,
	} {
		slices.SortFunc(*tests, compareTests)
	}
	slices.Sort(result.Failed)
	slices.Sort(result.Skipped)
}

func (l *ledger) timings() Timings {
	timings := Timings{Packages: make(map[string]PackageTiming)}
	for test, elapsed := range l.elapsed {
		measured := timings.Packages[test.Package]
		if measured.Tests == nil {
			measured.Tests = make(map[string]int64)
		}
		measured.Tests[test.Name] = elapsed
		timings.Packages[test.Package] = measured
	}
	for pkg, elapsed := range l.packageMS {
		measured := timings.Packages[pkg]
		measured.ElapsedMS = elapsed
		timings.Packages[pkg] = measured
	}
	return timings
}

// findingLimit caps how many names one finding prints; the rest are counted.
const findingLimit = 20

func findings(result Result) []error {
	var failures []error
	add := func(what string, names []string) {
		if len(names) == 0 {
			return
		}
		shown := names[:min(len(names), findingLimit)]
		message := fmt.Sprintf("%d %s: %s", len(names), what, strings.Join(shown, ", "))
		if len(names) > len(shown) {
			message += fmt.Sprintf(" and %d more", len(names)-len(shown))
		}
		failures = append(failures, errors.New(message))
	}
	add("listed tests never ran", testNames(result.Missing))
	add("tests ran that the contour does not list", testNames(result.Unexpected))
	add("tests ran more than once", testNames(result.Repeated))
	add("tests ran on a shard the partition did not give them", testNames(result.Misplaced))
	add("tests started and never finished", testNames(result.Incomplete))
	add("tests or subtests skipped", result.Skipped)
	add("tests or packages failed", result.Failed)
	return failures
}

func testNames(tests []Test) []string {
	names := make([]string, len(tests))
	for index, test := range tests {
		names[index] = test.String()
	}
	return names
}
