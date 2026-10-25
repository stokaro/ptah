package providerhost

import "slices"

// negotiated reports whether the provider and this engine both list a
// two-way feature the host lists too. The negotiated description holds only
// the features the host listed, so a listed one is negotiated.
func (t *target) negotiated(feature string) bool {
	return slices.Contains(t.host.description.GetFeatures(), feature) && slices.Contains(t.engine.GetFeatures(), feature)
}
