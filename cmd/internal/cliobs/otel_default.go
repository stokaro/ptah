//go:build !observability

package cliobs

import (
	"context"

	"github.com/stokaro/ptah/migration/migrator"
)

func startOTel(context.Context, Options) (migrator.Observer, func(context.Context) error, error) {
	return nil, nil, nil
}
