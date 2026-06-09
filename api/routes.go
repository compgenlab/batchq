// Package api defines the REST contract between batchq clients and the
// batchq server. It contains route paths and JSON request/response types.
// The package has no dependency on storage or transport; both client and
// server import it.
package api

// Version is the current major API version. It appears in route paths and
// in the X-Batchq-API-Version header on responses so clients can detect
// breaking changes.
const Version = "v1"

// Prefix is the path prefix every API route lives under.
const Prefix = "/api/" + Version

// Header names used on the wire.
const (
	HeaderVersion       = "X-Batchq-API-Version"
	HeaderAuthorization = "Authorization"

	// HeaderInternalOwner marks a request as coming from the server's
	// own ownership-monitor goroutine (server self-dials its socket to
	// confirm path → process binding). Activity tracking skips
	// requests with this header so the monitor's pings don't keep an
	// otherwise-idle server alive.
	HeaderInternalOwner = "X-Batchq-Internal-Owner"

	// HeaderDraining is set (to "1") on the 503 response a server returns
	// while it is shutting down (idle cull / drain). It marks the request
	// as having been rejected BEFORE any processing, so the client knows it
	// is safe to reconnect (autospawning a fresh server) and retry — turning
	// the rare idle-handoff collision into a transparent reconnect instead
	// of a hard error.
	HeaderDraining = "X-Batchq-Draining"
)

// Route paths (relative to Prefix). Path parameters use {} markers in the
// pattern strings; the server router fills them in.
const (
	RouteHealth = "/healthz"

	RouteJobs           = "/jobs"
	RouteJobsArray      = "/jobs/array"
	RouteJobsByID       = "/jobs/{id}"
	RouteArrayCancel    = "/arrays/{array_id}/cancel"
	RouteArrayHold      = "/arrays/{array_id}/hold"
	RouteArrayRelease   = "/arrays/{array_id}/release"
	RouteJobDependents  = "/jobs/{id}/dependents"
	RouteJobHold        = "/jobs/{id}/hold"
	RouteJobRelease     = "/jobs/{id}/release"
	RouteJobPriority    = "/jobs/{id}/priority"
	RouteJobCleanup     = "/jobs/{id}/cleanup"

	RouteQueue       = "/queue"
	RouteQueueCounts = "/queue/counts"

	RouteRunnerClaim         = "/runners/{runner_id}/claim"
	RouteRunnerClaimArray    = "/runners/{runner_id}/claim-array"
	RouteRunnerJobProxy      = "/runners/{runner_id}/jobs/{id}/proxy"
	RouteRunnerJobRunning    = "/runners/{runner_id}/jobs/{id}/running"
	RouteRunnerJobEnd        = "/runners/{runner_id}/jobs/{id}/end"
	RouteRunnerJobProxyEnd   = "/runners/{runner_id}/jobs/{id}/proxy-end"

	// RouteShutdown asks a local server to drain in-flight requests and
	// stop. Intended for local-socket use; remote deployments should
	// gate /admin/* at the reverse proxy.
	RouteShutdown = "/admin/shutdown"
)
