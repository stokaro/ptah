package migrator

import (
	"errors"
	"fmt"
)

// MaxRunReferenceLength is the longest run reference the logs store: the
// width of their run_reference column.
const MaxRunReferenceLength = 255

// MaxActorOriginLength is the longest actor origin the logs store: the width
// of their actor_origin column.
const MaxActorOriginLength = 64

// ErrInvalidRunAttribution is what [ValidateRunReference] and
// [ValidateActorOrigin] wrap when a value is not one the logs store as given.
var ErrInvalidRunAttribution = errors.New("invalid run attribution")

// ValidateRunReference reports whether the logs can store reference as given:
// 1 to [MaxRunReferenceLength] characters, each a visible ASCII character, so
// no engine's charset or collation can change it on the way in. An operation
// ID such as `sha256:` and 64 hexadecimal digits is one. Empty is valid and
// means no reference.
//
// The bound is a refusal rather than a cut: a shortened reference names
// another run, which is worse than none.
func ValidateRunReference(reference string) error {
	if reference == "" {
		return nil
	}
	if len(reference) > MaxRunReferenceLength {
		return fmt.Errorf("%w: a run reference is at most %d characters, and this one has %d",
			ErrInvalidRunAttribution, MaxRunReferenceLength, len(reference))
	}
	for index := range len(reference) {
		if character := reference[index]; character < '!' || character > '~' {
			return fmt.Errorf("%w: a run reference holds visible ASCII characters only, and this one has byte 0x%02x at offset %d",
				ErrInvalidRunAttribution, character, index)
		}
	}
	return nil
}

// ValidateActorOrigin reports whether the logs can store origin as given: one
// word of 1 to [MaxActorOriginLength] lowercase ASCII letters, digits, '.',
// '_', ':' and '-', starting with a letter or a digit, such as "admission".
// Empty is valid and means no origin.
//
// The word is the caller's and Ptah gives it no meaning. Keeping it to one
// spelling of one token is what lets a reader group entries by it.
func ValidateActorOrigin(origin string) error {
	if origin == "" {
		return nil
	}
	if len(origin) > MaxActorOriginLength {
		return fmt.Errorf("%w: an actor origin is at most %d characters, and this one has %d",
			ErrInvalidRunAttribution, MaxActorOriginLength, len(origin))
	}
	for index := range len(origin) {
		character := origin[index]
		word := character >= 'a' && character <= 'z' || character >= '0' && character <= '9'
		if !word && (index == 0 || character != '.' && character != '_' && character != ':' && character != '-') {
			return fmt.Errorf("%w: an actor origin is one lowercase word of letters, digits, '.', '_', ':' and '-' "+
				"that starts with a letter or a digit, and %q is not", ErrInvalidRunAttribution, origin)
		}
	}
	return nil
}
