package compare

import (
	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/routineargs"
)

// routinePair is one declared routine matched to the recorded routine it is.
type routinePair struct {
	declared schemamodel.Function
	recorded catalog.Function
}

// pairRoutineOverloads matches the routines a schema declares under one name to
// the routines a database records under it, and reports how many on each side
// are left over.
//
// The signature is consulted ONLY when a name carries more than one routine on
// either side, and that restriction is the whole safety argument. A name with
// one routine on each side pairs exactly as it always did, so the overwhelmingly
// common case is untouched and cannot regress on a signature this normalizer
// spells differently from the catalog. An overloaded name is the case that is
// already broken: both maps kept one entry per name, so the second overload
// overwrote the first and a dropped overload was reported as a modification of
// the survivor rather than as a removal (stokaro/ptah#1664).
//
// Left-over routines are genuine additions and removals. A normalizer that
// spelled one side differently would surface as an add beside a remove, which
// is visible in a plan, rather than as a silent mispairing.
func pairRoutineOverloads(
	declared []schemamodel.Function,
	recorded []catalog.Function,
) (pairs []routinePair, added []schemamodel.Function, removed []catalog.Function) {
	if len(declared) == 0 {
		return nil, nil, recorded
	}
	if len(recorded) == 0 {
		return nil, declared, nil
	}
	if len(declared) == 1 && len(recorded) == 1 {
		return []routinePair{{declared: declared[0], recorded: recorded[0]}}, nil, nil
	}

	used := make([]bool, len(recorded))
	pairs = make([]routinePair, 0, len(declared))
	for _, function := range declared {
		index := matchRecordedRoutine(function, recorded, used)
		if index < 0 {
			added = append(added, function)
			continue
		}
		used[index] = true
		pairs = append(pairs, routinePair{declared: function, recorded: recorded[index]})
	}
	// The routines themselves rather than a count, on BOTH sides. A removal has
	// to name the overload it removes, and only the recorded routine carries
	// the signature that does; returning how MANY were removed was enough to
	// plan `DROP FUNCTION IF EXISTS f`, which PostgreSQL refuses with
	// `function name "f" is not unique` whenever there is more than one
	// (stokaro/ptah#2296).
	//
	// The addition side was left as a count then, because the diff carried a
	// name and a name was all it needed. That was wrong for the same reason in
	// the other direction: two declared overloads appended the same name twice,
	// the planner resolved it by exact match to the same declaration both
	// times, and one overload was created twice while the other was never
	// created at all -- with the migration reporting success, because a second
	// `CREATE OR REPLACE` is a no-op (stokaro/ptah#2408).
	for index, taken := range used {
		if !taken {
			removed = append(removed, recorded[index])
		}
	}
	return pairs, added, removed
}

// matchRecordedRoutine finds the unused recorded routine whose signature equals
// the declared one's, or -1.
//
// Only the PostgreSQL reader captures identity arguments, so on every other
// dialect [catalog.Function.Signature] is the declared parameters. That is why
// the caller consults a signature at all only where a name is overloaded: the
// two sides are then compared on the same kind of string, which is the best
// available and is still better than keeping one entry per name.
func matchRecordedRoutine(declared schemamodel.Function, recorded []catalog.Function, used []bool) int {
	want := routineargs.Signature(declared.Parameters)
	for index, candidate := range recorded {
		if used[index] {
			continue
		}
		if routineargs.Signature(candidate.Signature()) == want {
			return index
		}
	}
	return -1
}
