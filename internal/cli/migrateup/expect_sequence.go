package migrateup

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"

	"ptah.run/migration/migrator"
)

// expectedSequence is the ordered set of migrations a caller approved, read
// from --expect-sequence. A run that selects anything else under the migration
// lock is refused before it changes the schema or the revision table.
//
// It exists for a caller that computed a plan against one reading of the
// history and has to know the run executes that plan and nothing more. A check
// the caller makes before the run is a different session and a different
// moment from the one the migrator selects in: a history restored backwards in
// between turns an approved [4] into a selected [2 3 4], and without this the
// two extra migrations run. The comparison therefore happens where the
// selection does, under the lock the run then mutates behind.
type expectedSequence struct {
	Migrations []expectedMigration `json:"migrations"`
	// Repeatables is the approved repeatable work, in the order the run
	// executes it. An absent list approves none, so a run that would execute
	// a repeatable migration the caller did not see is refused like one that
	// selects an extra version.
	Repeatables []expectedRepeatable `json:"repeatables,omitempty"`
}

// expectedRepeatable is one approved repeatable migration: its identity, the
// digest of the content the caller saw, and optionally the digest the caller
// saw recorded as applied for that identity.
//
// AppliedChecksum is absent when the caller makes no claim about the record,
// and then the record is not compared. Present, it is the digest the run must
// find recorded under the lock, and an empty string says the caller saw no
// record. An approval that shows "changed from X" binds X: another writer that
// applies a third version between the caller's read and the run moves the
// record, and the approved body would otherwise run over a state the approver
// never saw.
type expectedRepeatable struct {
	Identity        string       `json:"identity"`
	Checksum        string       `json:"checksum"`
	AppliedChecksum appliedClaim `json:"applied_checksum"`
}

// appliedClaim is the applied digest an approval binds, and whether it binds
// one at all. JSON null is refused rather than read as either: a caller that
// writes null may mean "no record", which is the empty string, or "no claim",
// which is an absent field, and guessing either way enforces a constraint the
// caller did not set or drops one it did.
type appliedClaim struct {
	bound  bool
	digest string
}

// UnmarshalJSON reads a string, and refuses null and any other JSON value.
func (a *appliedClaim) UnmarshalJSON(content []byte) error {
	if string(bytes.TrimSpace(content)) == "null" {
		return errors.New("applied_checksum is null: write the digest, an empty string for no record, " +
			"or leave the field out to make no claim about the record")
	}
	if err := json.Unmarshal(content, &a.digest); err != nil {
		return fmt.Errorf("applied_checksum: %w", err)
	}
	a.bound = true
	return nil
}

// approved is the entry as the comparison names it: identity@checksum, and
// where the entry binds the record, the digest it runs over, or "nothing" for
// no record.
func (r expectedRepeatable) approved() string {
	if !r.AppliedChecksum.bound {
		return r.Identity + "@" + r.Checksum
	}
	return r.Identity + "@" + r.Checksum + " over " + recordedOrNothing(r.AppliedChecksum.digest)
}

// recordedOrNothing spells an applied digest for a comparison, and an empty
// one, which no record has, as "nothing".
func recordedOrNothing(applied string) string {
	if applied == "" {
		return "nothing"
	}
	return applied
}

// expectedMigration is one approved migration, named the way the revision
// table names it: by its version key when the entry carries one, and by the
// decimal version otherwise. An entry with a key needs no version, because in
// an Atlas directory whose files run under their positions the run document
// prints none.
type expectedMigration struct {
	Version    int64  `json:"version"`
	VersionKey string `json:"version_key"`
}

// SequenceMismatchError reports a selection that is not the approved one. It
// carries both lists, because which migrations appeared or went missing is what
// tells the caller how the history moved.
type SequenceMismatchError struct {
	Expected []string
	Selected []string
}

// Error names both lists and says nothing ran.
func (e *SequenceMismatchError) Error() string {
	return fmt.Sprintf(
		"the migrations selected under the migration lock are [%s], and the approved sequence is [%s]: "+
			"the history moved after the sequence was approved, so nothing was run",
		strings.Join(e.Selected, " "), strings.Join(e.Expected, " "))
}

// loadExpectedSequence reads --expect-sequence. An empty path means the flag
// was not given and nothing is compared; a file that is given must parse
// strictly, because a field this build does not read would be a constraint the
// caller believes it set and the run would not enforce.
func loadExpectedSequence(path string) (*expectedSequence, error) {
	if path == "" {
		return nil, nil
	}
	content, err := os.ReadFile(path) // #nosec G304 -- the path is the caller's own argument
	if err != nil {
		return nil, fmt.Errorf("read --%s: %w", expectSequenceFlag, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.DisallowUnknownFields()
	var sequence expectedSequence
	if err := decoder.Decode(&sequence); err != nil {
		return nil, fmt.Errorf("parse --%s: %w", expectSequenceFlag, err)
	}
	if decoder.More() {
		return nil, fmt.Errorf("parse --%s: the file holds more than one document", expectSequenceFlag)
	}
	if sequence.Migrations == nil {
		return nil, fmt.Errorf("parse --%s: the document names no migrations list", expectSequenceFlag)
	}
	for index, migration := range sequence.Migrations {
		if migration.VersionKey == "" && migration.Version < 1 {
			return nil, fmt.Errorf("parse --%s: migration %d has version %d, which is not positive, and no version_key",
				expectSequenceFlag, index, migration.Version)
		}
	}
	for index, repeatable := range sequence.Repeatables {
		if repeatable.Identity == "" || repeatable.Checksum == "" {
			return nil, fmt.Errorf("parse --%s: repeatable %d needs both an identity and a checksum",
				expectSequenceFlag, index)
		}
	}
	return &sequence, nil
}

// refuseDisagreeingNumbers refuses an entry whose number and decimal key name
// two different migrations, such as version 99 beside version_key 0001, which
// names migration 1. The comparison matches on the key alone, so without this
// the 99 would be accepted and ignored. Where the directory runs its files
// under their positions (see [migrator.Migrator.KeyedByPosition]) the number
// beside a key is a position, which an approval written in another run may
// spell differently, and only the key names the migration. A nil sequence
// refuses nothing.
func (e *expectedSequence) refuseDisagreeingNumbers(mig interface{ KeyedByPosition() bool }) error {
	if e == nil || mig.KeyedByPosition() {
		return nil
	}
	for index, migration := range e.Migrations {
		number, err := strconv.ParseInt(migration.VersionKey, 10, 64)
		if migration.Version != 0 && err == nil && number != migration.Version {
			return fmt.Errorf("parse --%s: migration %d has version %d and version_key %q, which name different migrations",
				expectSequenceFlag, index, migration.Version, migration.VersionKey)
		}
	}
	return nil
}

// RepeatableSequenceMismatchError reports repeatable work under the lock that
// is not the approved work: another identity, another order, or content whose
// digest is not the one the caller saw.
type RepeatableSequenceMismatchError struct {
	Expected []string
	Selected []string
}

// Error names both lists, each entry as identity@checksum followed, where the
// approval binds the record, by the applied digest it runs over, and says
// nothing ran.
func (e *RepeatableSequenceMismatchError) Error() string {
	return fmt.Sprintf(
		"the repeatable migrations selected under the migration lock are [%s], and the approved repeatables are "+
			"[%s]: the directory or the record moved after the sequence was approved, so nothing was run",
		strings.Join(e.Selected, " "), strings.Join(e.Expected, " "))
}

// check compares the selection with the approved sequence, element by element
// and in order. The order is part of the approval: the same migrations in
// another order are a different run.
func (e *expectedSequence) check(plan migrator.MigrationPlan) error {
	expected := make([]string, 0, len(e.Migrations))
	for _, migration := range e.Migrations {
		expected = append(expected, identity(migration.Version, migration.VersionKey))
	}
	selected := make([]string, 0, len(plan.Versions))
	for index, version := range plan.Versions {
		key := ""
		if index < len(plan.VersionKeys) {
			key = plan.VersionKeys[index]
		}
		selected = append(selected, identity(version, key))
	}
	if !slices.Equal(expected, selected) {
		return &SequenceMismatchError{Expected: expected, Selected: selected}
	}
	expectedRepeatables := make([]string, 0, len(e.Repeatables))
	for _, repeatable := range e.Repeatables {
		expectedRepeatables = append(expectedRepeatables, repeatable.approved())
	}
	// A selected entry names its record where the approved entry at the same
	// place binds one, so an approval that makes no claim about the record
	// compares identity and checksum alone.
	selectedRepeatables := make([]string, 0, len(plan.Repeatables))
	for index, work := range plan.Repeatables {
		selected := work.Identity + "@" + work.Checksum
		if index < len(e.Repeatables) && e.Repeatables[index].AppliedChecksum.bound {
			selected += " over " + recordedOrNothing(work.AppliedChecksum)
		}
		selectedRepeatables = append(selectedRepeatables, selected)
	}
	if !slices.Equal(expectedRepeatables, selectedRepeatables) {
		return &RepeatableSequenceMismatchError{Expected: expectedRepeatables, Selected: selectedRepeatables}
	}
	return nil
}

// guard is the check as the migrator runs it: under the lock, for every
// selection including an empty one. A nil sequence guards nothing.
func (e *expectedSequence) guard() migrator.MigrationPlanGuard {
	if e == nil {
		return nil
	}
	return func(_ context.Context, plan migrator.MigrationPlan) error {
		return e.check(plan)
	}
}

// identity is how one migration is named for comparison: its version key when
// it has one, and the decimal version when it does not, so an absent key means
// the numeric identity rather than a wildcard. The number beside a key takes no
// part: in an Atlas directory whose files run under their positions it is a
// position, which moves when a file is added before it, and an approval
// written in one run would name another migration in the next.
func identity(version int64, key string) string {
	if key == "" {
		return strconv.FormatInt(version, 10)
	}
	return key
}
