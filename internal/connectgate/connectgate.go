// Package connectgate lets a command surface refuse a database dialect for
// every connection opened under a context, before anything is dialed.
//
// A surface that does not reach a dialect yet cannot list every path that
// opens a connection: a URL arrives through a flag, an environment variable, a
// project file, a data source the project file reads, or a command the surface
// forwards to. So the surface puts its refusal on the context it runs under,
// and dbschema's connector asks it about the dialect of every URL it is handed.
// The refusal holds for whichever path the URL took, and a path added later is
// covered without being named.
package connectgate

import "context"

// Refusal decides whether a connection to dialect may open: nil lets it open,
// and an error refuses it with that error. dialect is the canonical name
// [ptah.run/core/platform.NormalizeDialect] gives the URL's scheme.
type Refusal func(dialect string) error

type refusalKey struct{}

// With returns a context under which refusal is asked about every connection.
// It replaces any refusal ctx already carries, so installing the same refusal
// on every run of a reused command tree does not stack it.
func With(ctx context.Context, refusal Refusal) context.Context {
	return context.WithValue(ctx, refusalKey{}, refusal)
}

// Check asks the refusal ctx carries about dialect. A nil context, or one
// that carries no refusal, refuses nothing.
func Check(ctx context.Context, dialect string) error {
	if ctx == nil {
		return nil
	}
	refusal, ok := ctx.Value(refusalKey{}).(Refusal)
	if !ok || refusal == nil {
		return nil
	}
	return refusal(dialect)
}
