// Package fsdurable provides filesystem operations for durable artifact
// publication across supported operating systems.
//
// PublishFileAt is conditional: the caller states the destination it expects
// through a Destination, and the commit primitive itself binds that state, so
// no time-of-check/time-of-use gap remains between a caller's validation and
// the rename. Losing the race yields ErrDestinationChanged with the rival's
// bytes untouched. Platforms and filesystems that cannot supply a conditional
// rename fail with ErrConditionalPublicationUnsupported rather than degrading
// to an unconditional one.
package fsdurable
