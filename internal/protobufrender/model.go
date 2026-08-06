package protobufrender

import (
	"cmp"
	"slices"
	"sort"
)

// The exporter never allocates these numbers: protoc rejects the 19000-19999
// range outright and 536870911 is the largest legal field number.
const (
	reservedRangeStart = 19000
	reservedRangeEnd   = 19999
	maxFieldNumber     = 536870911
)

// field is one message field in the generated file.
type field struct {
	Name     string
	Number   int32
	Type     string
	Repeated bool
	Comment  string
}

// enumValue is one value of a generated enum.
type enumValue struct {
	Name    string
	Number  int32
	Comment string
}

// reservations are the numbers and names a type may never reuse. They are what
// buys the WIRE_JSON compatibility guarantee: buf reports a removed field as
// breaking unless both its number and its name are reserved.
//
// Numbers are held as ranges, the same shape a .proto spells them in. A single
// legal range covers the whole 536,870,911-number field space, so expanding one
// into individual numbers would make both loading and storage proportional to
// the width of the reservation rather than to the number of ranges.
type reservations struct {
	Ranges []numberRange
	Names  []string
}

// hasNumbers reports whether the type reserves any number at all.
func (r *reservations) hasNumbers() bool {
	return len(r.Ranges) > 0
}

// highest returns the largest reserved number, or 0 when nothing is reserved.
func (r *reservations) highest() int32 {
	var highest int32
	for _, rng := range r.Ranges {
		highest = max(highest, rng.End)
	}
	return highest
}

func (r *reservations) addNumber(n int32) {
	r.addRange(n, n)
}

// addRange records [start, end] inclusive. Overlaps and duplicates are folded
// away by normalizeRanges before the model is written, so callers may add the
// same number twice.
func (r *reservations) addRange(start, end int32) {
	r.Ranges = append(r.Ranges, numberRange{Start: start, End: end})
}

func (r *reservations) addName(name string) {
	if slices.Contains(r.Names, name) {
		return
	}
	r.Names = append(r.Names, name)
}

func (r *reservations) hasName(name string) bool {
	return slices.Contains(r.Names, name)
}

func (r *reservations) dropName(name string) {
	r.Names = slices.DeleteFunc(r.Names, func(existing string) bool { return existing == name })
}

func (r *reservations) sort() {
	r.Ranges = normalizeRanges(r.Ranges)
	slices.Sort(r.Names)
}

// message is a generated Protobuf message.
type message struct {
	Name     string
	Comment  string
	Fields   []field
	Reserved reservations
	// Tombstone marks a message retained only so its numbers and names stay
	// reserved after the source table disappeared.
	Tombstone bool
}

// enum is a generated Protobuf enum.
type enum struct {
	Name     string
	Comment  string
	Values   []enumValue
	Reserved reservations
	// Tombstone marks an enum retained only for wire compatibility. Unlike a
	// message, an enum can never be emptied: protoc rejects an enum with no
	// values, so a tombstoned enum keeps its synthesized zero value.
	Tombstone bool
}

// file is the whole generated compilation unit.
type file struct {
	Package   string
	GoPackage string
	Imports   []string
	Messages  []message
	Enums     []enum
}

// sortForOutput puts the model into the deterministic order the writer emits:
// types by name, fields and enum values by number ascending so that reordering
// a database column never produces a diff, and reservations ascending.
func (f *file) sortForOutput() {
	sort.Slice(f.Messages, func(i, j int) bool { return f.Messages[i].Name < f.Messages[j].Name })
	sort.Slice(f.Enums, func(i, j int) bool { return f.Enums[i].Name < f.Enums[j].Name })
	for i := range f.Messages {
		m := &f.Messages[i]
		sort.Slice(m.Fields, func(a, b int) bool { return m.Fields[a].Number < m.Fields[b].Number })
		m.Reserved.sort()
	}
	for i := range f.Enums {
		e := &f.Enums[i]
		sort.Slice(e.Values, func(a, b int) bool { return e.Values[a].Number < e.Values[b].Number })
		e.Reserved.sort()
	}
	sort.Strings(f.Imports)
}

// numberRange is a contiguous run of reserved numbers, inclusive at both ends.
type numberRange struct {
	Start int32
	End   int32
}

// normalizeRanges sorts ranges ascending and folds overlapping or adjacent runs
// into one, so two separately retired neighbors are written as "reserved 2 to
// 3;" rather than "reserved 2, 3;". Merging is a correctness requirement and not
// only formatting: protoc refuses a file whose reserved ranges overlap, and the
// same number can reach the model from both a removed field and a range the
// previous file already carried.
func normalizeRanges(in []numberRange) []numberRange {
	if len(in) == 0 {
		return nil
	}
	sorted := slices.Clone(in)
	slices.SortFunc(sorted, func(a, b numberRange) int {
		if a.Start != b.Start {
			return cmp.Compare(a.Start, b.Start)
		}
		return cmp.Compare(a.End, b.End)
	})

	out := []numberRange{sorted[0]}
	for _, rng := range sorted[1:] {
		last := &out[len(out)-1]
		// Compared in int64 so an end of the maximum field number cannot
		// overflow the adjacency test.
		if int64(rng.Start) <= int64(last.End)+1 {
			last.End = max(last.End, rng.End)
			continue
		}
		out = append(out, rng)
	}
	return out
}

// nextNumber returns the next free field number for a type, given the highest
// number it has ever used. Gaps are never filled and the protobuf implementation
// range is skipped, so a number retired by a removed column can never come back.
func nextNumber(highest int32) (int32, bool) {
	next := highest + 1
	if next >= reservedRangeStart && next <= reservedRangeEnd {
		next = reservedRangeEnd + 1
	}
	if next > maxFieldNumber {
		return 0, false
	}
	return next, true
}
