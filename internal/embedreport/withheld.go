package embedreport

// withheld names every field of a stored run this package does not report, and
// why.
//
// The list is not documentation. `TestStatusOf_EveryStoredFieldIsReportedOrWithheld`
// enumerates the stored type by reflection and fails when a field is in neither
// this map nor the status, so a field added upstream cannot arrive on the agent
// surface by being added, and cannot be dropped from an operator's view by being
// forgotten. Somebody has to decide, once, and write the reason here.
//
// The decision this exists for is Cursor. It is the resume position of a
// backfill, and a resume position over a keyed source is a list of source key
// values -- row identities, which are content. It is the one field here whose
// absence a reader would otherwise read as an oversight.
var withheld = map[string]string{
	"Cursor": "the backfill's resume position is a list of source key values, which are " +
		"row identities rather than progress",
	"Source": "the fully qualified source relation is in the plan's facts, where it is " +
		"reported with its provenance rather than as a bare string",
	"Target": "likewise the target relation",
	"ProviderProfile": "the profile name is an operator's local configuration key and " +
		"identifies nothing about the run",
	"ResolvedModel": "the model is in the plan's disclosure, which is where a reader " +
		"deciding about what leaves the system looks",
	"PtahVersion": "which build wrote the row says nothing about the run's state, and a " +
		"version is a fingerprint of the host",
	"PolicyDigest": "the policy is what refuses a cutover, and its refusals are reported " +
		"as refusals; the digest of the policy document is not an answer to anything " +
		"asked here",
	"CreatedAt": "when the run was started answers nothing UpdatedAt does not; a run that " +
		"has not moved is told by when it last moved",
}
