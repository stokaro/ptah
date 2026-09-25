package migrateup

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
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
}

// expectedMigration is one approved migration, named the way the revision
// table names it.
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
		if migration.Version < 1 {
			return nil, fmt.Errorf("parse --%s: migration %d has version %d, which is not positive",
				expectSequenceFlag, index, migration.Version)
		}
	}
	return &sequence, nil
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
	if len(expected) != len(selected) {
		return &SequenceMismatchError{Expected: expected, Selected: selected}
	}
	for index := range expected {
		if expected[index] != selected[index] {
			return &SequenceMismatchError{Expected: expected, Selected: selected}
		}
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

// identity is how one migration is named for comparison. A revision row keeps
// its version key when the directory gave one and the decimal version when it
// did not, so an absent key means the numeric identity rather than a wildcard.
func identity(version int64, key string) string {
	if key == "" {
		key = strconv.FormatInt(version, 10)
	}
	if key == strconv.FormatInt(version, 10) {
		return key
	}
	return strconv.FormatInt(version, 10) + "/" + key
}
