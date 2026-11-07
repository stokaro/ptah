package conformance

// CheckInfo describes one check of the suite.
type CheckInfo struct {
	// ID names the check in a Report. A check run once per call or change
	// adds a dot and the call's or change's name.
	ID string `json:"id"`
	// Summary says what the check requires of a provider.
	Summary string `json:"summary"`
	// Live reports a check that runs only with Config.Live.
	Live bool `json:"live"`
}

// The check identifiers. They are stable: a report consumer may match them.
const (
	checkBootstrap         = "bootstrap.start"
	checkOtherMajor        = "handshake.other-major"
	checkContractMajor     = "handshake.contract-major"
	checkHostFeatures      = "handshake.host-features"
	checkRequiredFeatures  = "handshake.required-features"
	checkFirstMinorHost    = "handshake.contract-25.0-host"
	checkMinimumContract   = "handshake.minimum-contract"
	checkDescriptionValid  = "description.valid"
	checkIdentity          = "description.identity"
	checkStable            = "description.stable"
	checkNegotiation       = "negotiation.description"
	checkAdvertised        = "services.advertised"
	checkUnadvertised      = "services.unadvertised"
	checkUnknownSession    = "sessions.unknown"
	checkContextCredential = "database-context.credentials"
	checkModels            = "models.recorded"
	checkCalls             = "calls"
	checkOpen              = "live.open"
	checkClose             = "live.close"
	checkSessions          = "live.sessions"
	checkRestart           = "live.restart"
	checkInspect           = "live.inspect"
	checkLocks             = "live.locks"
	checkTransactions      = "live.transactions"
	checkOpenCredential    = "live.credentials"
	checkChanges           = "live.changes"
	checkRefused           = "live.refused"
	checkLiveCalls         = "live.calls"
	checkReasons           = "reasons.negotiated"
	checkRedacted          = "reasons.redacted"
)

// Checks returns every check of the suite, in the order a run reports them.
func Checks() []CheckInfo {
	return []CheckInfo{
		{ID: checkBootstrap, Summary: "The provider starts and answers Describe: an executable through the checksum-pinned bootstrap, an in-process server through the checks Serve applies."},
		{ID: checkOtherMajor, Summary: "A host of another contract major is refused."},
		{ID: checkContractMajor, Summary: "A host whose contract major differs from its revision is refused."},
		{ID: checkHostFeatures, Summary: "A repeated or malformed host feature is refused, and a feature the provider does not know is ignored."},
		{ID: checkRequiredFeatures, Summary: "A host that requires a feature the provider does not offer is refused."},
		{ID: checkFirstMinorHost, Summary: "A host that announces no contract version receives the description of contract 25.0, or a refusal when the provider's minimum is higher."},
		{ID: checkMinimumContract, Summary: "The provider's minimum contract is one this SDK speaks."},
		{ID: checkDescriptionValid, Summary: "The description passes the SDK's validation under every host profile, with the required features."},
		{ID: checkIdentity, Summary: "The description names the provider identity the manifest names."},
		{ID: checkStable, Summary: "Two starts describe the provider identically, model identities included."},
		{ID: checkNegotiation, Summary: "Under every host profile, the description is the full one without the two-way features the host did not list."},
		{ID: checkAdvertised, Summary: "Every RPC of a service the description offers answers something other than Unimplemented."},
		{ID: checkUnadvertised, Summary: "No RPC of a service the description does not offer succeeds."},
		{ID: checkUnknownSession, Summary: "A session-bound call that names a session the provider never opened is refused."},
		{ID: checkContextCredential, Summary: "Resolving the database context of the credential address returns nothing that quotes the canary."},
		{ID: checkModels, Summary: "Every recorded model identity still reads, as a model the provider describes or as a predecessor one names."},
		{ID: checkCalls, Summary: "An offline call answers within the contract: a reply the SDK's validator accepts under every host profile and the same reply each time, or the refusal the call expects."},
		{ID: checkOpen, Summary: "Opening the address returns a valid session under every host profile.", Live: true},
		{ID: checkClose, Summary: "Closing a session succeeds, closing it again succeeds, and the closed handle is refused.", Live: true},
		{ID: checkSessions, Summary: "Two sessions have distinct handles, and closing one leaves the other usable.", Live: true},
		{ID: checkRestart, Summary: "A session handle of a process that exited is refused by a new process.", Live: true},
		{ID: checkInspect, Summary: "Reading the database returns a snapshot the SDK's validator accepts under every host profile, the same snapshot each time.", Live: true},
		{ID: checkLocks, Live: true,
			Summary: "A lock excludes a session of another process (under Live.InProcessLocks, another session of its own process, and no other process opens the database), " +
				"a lock ID is used once, releasing an unused ID fences it, and closing a session releases its locks."},
		{ID: checkTransactions, Summary: "A transaction begins with the isolation it asked for, a finish repeats its stored result, and rolling back an unused ID fences it.", Live: true},
		{ID: checkOpenCredential, Summary: "Opening the credential address returns nothing that quotes the canary.", Live: true},
		{ID: checkChanges, Summary: "A change applies with every unit confirmed, reverts, and the database then reads as it did before.", Live: true},
		{ID: checkRefused, Summary: "A refused unit is NOT_APPLIED with nothing applied, and a confirmed prefix is what the database holds.", Live: true},
		{ID: checkLiveCalls, Summary: "A session-bound call answers within the contract: a valid reply, or the refusal the call expects.", Live: true},
		{ID: checkReasons, Summary: "A refusal or a result carries a reason only when both sides list errors.reasons, and then exactly one reason of the valid shape."},
		{ID: checkRedacted, Summary: "No reason, message or session field the provider sends quotes the live address, its credential or the canary."},
	}
}
