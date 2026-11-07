package provider

// DeclaredObjectsFeature enables ModelComparison.declared_kinds and
// CompareModelsRequest.declared_objects: a comparison names the object
// models of its engine whose declared objects it reads, and the host sends
// it the identity of each. A two-way feature: the provider lists it, and
// so does each engine with a comparison that names such kinds. Contract
// 25.33.
const DeclaredObjectsFeature = "models.declared-objects"
