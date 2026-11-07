package provider

// ViewQueryEquivalenceFeature enables CompareViewQueries: the owner of an
// engine decides whether the query a declared view states and the query
// the held view keeps are one query, in place of the host's comparison of
// their texts. The call is new and adds nothing to an existing message, so
// the feature is the provider's alone: the provider lists it, and so does
// each engine whose view queries it compares. Contract 25.33.
const ViewQueryEquivalenceFeature = "schema.view-query-equivalence"
