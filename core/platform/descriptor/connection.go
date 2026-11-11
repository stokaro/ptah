package descriptor

import "fmt"

// validateConnection checks the fields a connection reads: the driver's name
// and the scheme the driver reads, each empty or a spelling in the alphabet of
// Name.
func (d Dialect) validateConnection() error {
	if d.VulnerabilityScan != "" && d.VulnerabilityScan != "supported" && d.VulnerabilityScan != "not_applicable" {
		return fmt.Errorf("%w: dialect %q: vulnerability scan state %q is invalid", ErrInvalidDescriptor, d.Name, d.VulnerabilityScan)
	}
	if d.Driver != "" && !validSpelling(d.Driver) {
		return fmt.Errorf("%w: dialect %q: driver %q must be lowercase ASCII letters, digits, '-', '_' or '+', beginning with a letter", ErrInvalidDescriptor, d.Name, d.Driver)
	}
	if d.WireScheme != "" && !validSpelling(d.WireScheme) {
		return fmt.Errorf("%w: dialect %q: wire scheme %q must be lowercase ASCII letters, digits, '-', '_' or '+', beginning with a letter", ErrInvalidDescriptor, d.Name, d.WireScheme)
	}
	return nil
}
