package fsdurable

import (
	"fmt"
	"io/fs"
	"os"
)

type destinationKind uint8

const (
	destinationUnset destinationKind = iota
	destinationAbsent
	destinationFile
)

// Destination is the state a rooted publication requires at its target name at
// the instant the commit takes effect. Publication is conditional on that
// state, so a concurrent writer that reaches the target first is reported
// rather than overwritten.
//
// The zero value expresses no expectation and is rejected: every publication
// must state what it replaces, which keeps "publish over anything" from being
// reachable by omission.
type Destination struct {
	kind destinationKind
	info fs.FileInfo
}

// ExpectAbsent requires that no entry exists at the target name.
func ExpectAbsent() Destination {
	return Destination{kind: destinationAbsent}
}

// ExpectFile requires that the target name still identifies the regular file
// captured by info through an os.Stat, os.Lstat or os.File.Stat operation.
// Identity, size and modification time must all still match, because inode
// numbers alone are reused by some filesystems.
func ExpectFile(info fs.FileInfo) Destination {
	return Destination{kind: destinationFile, info: info}
}

func (d Destination) validate(targetName string) error {
	switch d.kind {
	case destinationAbsent:
		return nil
	case destinationFile:
		if d.info == nil {
			return destinationChangedError(targetName, errNilDestinationIdentity)
		}
		if !d.info.Mode().IsRegular() {
			return destinationChangedError(targetName, errIrregularDestinationIdentity)
		}
		return nil
	case destinationUnset:
	}
	return destinationChangedError(targetName, errUnstatedDestination)
}

// matches reports whether actual is still the file the caller expected. Mode is
// deliberately excluded: publication applies the final mode to the staged file,
// so a caller that expects the file it published earlier compares against the
// staged identity whose recorded mode predates that change.
func (d Destination) matches(actual fs.FileInfo) bool {
	return d.kind == destinationFile &&
		actual != nil &&
		actual.Mode().IsRegular() &&
		os.SameFile(actual, d.info) &&
		actual.Size() == d.info.Size() &&
		actual.ModTime().Equal(d.info.ModTime())
}

var (
	errNilDestinationIdentity       = fmt.Errorf("expected destination identity is nil")
	errIrregularDestinationIdentity = fmt.Errorf("expected destination is not a regular file")
	errUnstatedDestination          = fmt.Errorf("publication states no destination expectation")
	errDisplacedDestination         = fmt.Errorf("the destination was replaced before the commit took effect")
)
