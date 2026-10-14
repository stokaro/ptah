//go:build !js

package oracleconnect

import (
	_ "github.com/sijms/go-ora/v3" // registers the oracle driver
)
