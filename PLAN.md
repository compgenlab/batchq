# batchq — plans and ideas

Working notes on direction. This is not a commitment or a roadmap with
dates — it's a place to capture ideas before they're lost, with enough
context that they can be picked up later.

## Potential plans for the future

### Root identity proxy → SLURM-free multi-user HPC cluster

**Idea.** Make batchq usable on its own as the scheduler for a small HPC
cluster — no SLURM underneath — by letting a trusted, root-owned batchq
"proxy" on the login/submit nodes capture kernel-attested identity and
forward it to a remote batchq-server.

**Why this needs solving.** Today identity is only trustworthy over the
local unix socket:

- A unix-socket server reads the peer's uid/gid from the kernel
  (`SO_PEERCRED`, `server/peercred.go`), and the submit path overrides
  whatever the client claimed with the kernel-attested values
  (`service/service.go:applyPeerIdentity`, fed by NSS). The client
  cannot spoof its uid/gid. Ownership checks on hold/release/cancel
  then key off that stored uid (root = admin).
- Over a **remote** connection (`[batchq] remote`, HTTPS) or a `tcp://`
  port there is no local root socket and therefore no peer credentials,
  so the client-supplied uid/gid are preserved as-is — trust-based, not
  kernel-attested. That's fine for a single-user remote, but it can't
  underpin a multi-user cluster.

The gap: a remote batchq-server can't independently verify *who* submitted
a job. SLURM solves this by being the privileged thing on the submit host.

**Sketch of the proxy.** Run a root-owned batchq process on each
login/submit node that:

1. Accepts local client connections on its `0600` unix socket and reads
   the kernel-attested uid/gid (the mechanism that already exists).
2. Forwards the request to the remote batchq-server over HTTPS, asserting
   the captured identity in a trusted, signed way (not as a spoofable
   request field). The hop is authenticated by a bearer token today; the
   identity assertion would need its own integrity (e.g. a header signed
   with a proxy key the server trusts, or mTLS where the server maps the
   client cert to "this is a trusted submit-node proxy allowed to assert
   identity"). The bearer token alone proves "a trusted proxy is calling"
   but should not double as "and here is an un-spoofable uid."
3. The remote server, seeing the request came from a trusted proxy,
   accepts the asserted uid/gid the same way the local server accepts
   peer creds today — i.e. `applyPeerIdentity` generalizes from "peer
   creds from this socket" to "identity vouched for by a trusted proxy."

**Compute side.** Root runners on the compute nodes (the simple runner
already requires root for cgroup enforcement) would drop privileges to
the job's stored uid/gid before exec — setgid + initgroups + setuid, in
that order — so each user's job runs as that user. The uid/gid to drop to
is already carried in `job_details`; the missing piece is the privilege
drop in the exec path plus the operational story (the runner host must
share the user namespace / NSS / filesystem identity with the submit
nodes, as any HPC cluster does).

**Trust boundary.** The whole scheme rests on the login-node proxy being
root and trusted, and on the proxy→server identity assertion being
integrity-protected (not just bearer-token-gated). Anyone who can forge
that assertion can submit as any user — so the assertion mechanism, key
management, and which proxies the server trusts are the security-critical
design, not an afterthought.

**Explicitly out of scope for this sketch** (real schedulers need these,
but they're separate problems): fair-share / priority scheduling,
multi-tenant queue partitioning and limits, accounting, preemption.
Multi-tenant + Postgres work already lives in the private `batchq-server`
repo; this proxy idea is the public single-binary's contribution to that
picture — it's how kernel-attested identity reaches that server from the
submit nodes.

**Reuses what already exists:** peer-cred capture (`server/peercred.go`),
identity override + NSS lookup (`service/service.go:applyPeerIdentity`),
uid-based ownership authz (`service/service.go`), the remote-client
transport and bearer-token auth (`client/`, `server/auth.go`), and the
root-capable simple runner (`runner/simple.go`).
