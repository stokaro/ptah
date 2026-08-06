package protobufrender

import (
	"slices"
	"sort"
)

// The exporter never allocates these numbers: protoc rejects the 19000-19999
// range outright and 536870911 is the largest legal field number.
const (
	reservedRangeStart = 19000
	reservedRangeEnd   = 19999
	maxFieldNumber     = 536870911

	// maxReservedNumbers bounds how many individual numbers one type's
	// reservations may expand to when a previous export is read back. The
	// exporter only ever writes ranges it built from individual numbers, so
	// reaching this cap means the input is not something this exporter produced;
	// the bound keeps a "reserved 1 to max" style range from allocating two
	// billion entries before the digest gate can reject the file.
	maxReservedNumbers = 1 << 20
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
type reservations struct {
	Numbers []int32
	Names   []string
}

func (r *reservations) addNumber(n int32) {
	if slices.Contains(r.Numbers, n) {
		return
	}
	r.Numbers = append(r.Numbers, n)
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

func (r *reservations) count() int {
	return len(r.Numbers)
}

func (r *reservations) dropName(name string) {
	r.Names = slices.DeleteFunc(r.Names, func(existing string) bool { return existing == name })
}

func (r *reservations) sort() {
	slices.Sort(r.Numbers)
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

// file is one generated compilation unit.
type file struct {
	// Name is the file's base name inside the package directory.
	Name string
	// Anchor marks the file --out names. It holds every generated enum and, in a
	// multi-file set, the manifest of the other files.
	Anchor bool
	// Siblings is the sorted inventory written into the anchor's header. It is
	// empty for every other file and for a single-file export.
	Siblings  []string
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

// collapseRanges folds a sorted number list into contiguous runs so the writer
// can emit "reserved 1 to 7;" instead of seven separate entries.
func collapseRanges(numbers []int32) []numberRange {
	if len(numbers) == 0 {
		return nil
	}
	sorted := slices.Clone(numbers)
	slices.Sort(sorted)

	ranges := []numberRange{{Start: sorted[0], End: sorted[0]}}
	for _, n := range sorted[1:] {
		last := &ranges[len(ranges)-1]
		switch n {
		case last.End:
			// duplicate, already covered
		case last.End + 1:
			last.End = n
		default:
			ranges = append(ranges, numberRange{Start: n, End: n})
		}
	}
	return ranges
}

// nextNumber returns the next free field number for a type, given every number
// it has ever used. Gaps are never filled and the protobuf implementation range
// is skipped, so a number retired by a removed column can never come back.
func nextNumber(used []int32) (int32, bool) {
	var highest int32
	for _, n := range used {
		if n > highest {
			highest = n
		}
	}
	next := highest + 1
	if next >= reservedRangeStart && next <= reservedRangeEnd {
		next = reservedRangeEnd + 1
	}
	if next > maxFieldNumber {
		return 0, false
	}
	return next, true
}
