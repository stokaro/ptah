package provider

// LargeMessagesFeature raises the bound on one message between the host and
// the provider from 4 MiB to 64 MiB, the bounds runtime.MaxMessageBytes and
// runtime.LargeMessageBytes name. A request carries every table of a schema
// with what its read and its declaration know of each, so a schema of a few
// hundred tables does not fit 4 MiB even when each model travels by
// reference. Contract 25.26. It is a two-way feature of the provider alone: a
// provider lists it, the host lists it in its host features, and the SDK's
// client sends a message larger than 4 MiB only when the negotiated
// description lists it. The SDK's server accepts 64 MiB from any host, since a
// host that does not list the feature sends no more than 4 MiB.
const LargeMessagesFeature = "transport.large-messages"
