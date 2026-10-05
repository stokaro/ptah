package entities

// Reporting is the pool the reporting group's queries run in, at most ten at
// once with twenty more waiting.
//
//ptah:schema:resourcepool name="reporting" concurrent_query_limit="10" queue_size="20" query_memory_limit_percent_per_node="25.5"
//ptah:schema:resourcepool:classifier name="reporting_group" resource_pool="reporting" member_name="reporters" rank="100"
type Reporting struct{}
