package main

import (
	"fmt"

	"golang.org/x/tools/go/packages"
)

// scanFloor is the fewest packages a run must load. Every rule answers from
// the packages the loader returns, and a load that stopped matching most of
// the module would report no finding and read as success. The figure is a
// source constant, so a regenerated baseline cannot lower it; the module holds
// several hundred more.
const scanFloor = 500

// checkScanFloor refuses a load too small for the rules to mean anything.
func checkScanFloor(loaded []*packages.Package) error {
	if len(loaded) < scanFloor {
		return fmt.Errorf("the rules read %d packages, below the floor of %d", len(loaded), scanFloor)
	}
	return nil
}
