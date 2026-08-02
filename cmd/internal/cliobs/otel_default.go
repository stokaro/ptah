//go:build !observability

package cliobs

import (
	"context"

	"go.5x5.cz/ptah/migration/migrator"
)

func startOTel(context.Context, Options) (migrator.Observer, func(context.Context) error, error) {
	return nil, nil, nil
}
