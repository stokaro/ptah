//go:build !observability

package cliobs

import (
	"context"

	"ptah.run/migration/migrator"
)

func startOTel(context.Context, Options) (migrator.Observer, func(context.Context) error, error) {
	return nil, nil, nil
}
