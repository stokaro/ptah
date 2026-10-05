package capabilityprobe

import (
	"context"
	"fmt"
	"path"
)

type pathChecker interface {
	PathExists(context.Context, string) (bool, error)
}

// directoryLeftover confirms removal through the scheme catalog, which covers
// every object kind, including objects with no partition statistics.
func (s *session) directoryLeftover(ctx context.Context) (Attempt, []string) {
	directory := path.Join(s.database, s.namespace)
	read := Attempt{Statement: "look up " + directory + " in the scheme directory tree"}
	checker, ok := s.conn.SchemaWriter().(pathChecker)
	if !ok {
		read.ServerErr = fmt.Sprintf("the connection's schema writer %T cannot look up a path", s.conn.SchemaWriter())
		return read, []string{"the directory " + directory + ", which the scheme service would not look up"}
	}
	exists, err := checker.PathExists(ctx, s.namespace)
	if err != nil {
		read.ServerErr = err.Error()
		return read, []string{"the directory " + directory + ", which the scheme service would not look up"}
	}
	read.Accepted = true
	if exists {
		return read, []string{"the directory " + directory}
	}
	return read, nil
}
