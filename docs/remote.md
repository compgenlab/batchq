# Remote access

batchq's server never terminates TLS. It binds a unix domain socket by
default, or a plain-HTTP TCP port (`tcp://host:port`) for containerized
deployments — but TLS, network ACLs, rate limiting, and access logging
are a reverse proxy's job, so batchq doesn't reinvent any of it.

This page covers how to put a reverse proxy in front of batchq, the
shared-token gate, the TCP-port option for containers, and how to
configure remote clients.

## When you need this

- You want users on their own laptops to submit jobs into a batchq
  instance running on a cluster login node.
- You want a runner on a compute node to talk to a batchq server on a
  different host (one batchq server, many compute nodes).
- You want the web UI accessible from a browser somewhere other than
  the host the server runs on. (Though for that, an SSH port-forward
  is often easier — see [web](web.md).)

If everything that talks to batchq is on the same host, you do not
need this page. The unix socket already works.

## The pieces

```
┌─────────────────────────┐   HTTPS    ┌────────────────────────────┐
│  remote client          │ ─────────► │  reverse proxy (nginx,     │
│  --remote=https://…     │            │  Caddy, Traefik, …)        │
│  $BATCHQ_TOKEN          │            │  terminates TLS, forwards  │
└─────────────────────────┘            │  to unix socket            │
                                       └──────────────┬─────────────┘
                                                      │
                                                      │ unix socket
                                                      ▼
                                       ┌──────────────────────────┐
                                       │  batchq server           │
                                       │  $BATCHQ_HOME/batchq.sock│
                                       └──────────────────────────┘
```

The proxy speaks HTTPS to the world and HTTP-over-unix to batchq. The
batchq server's authentication model is unchanged: anyone who can write
to the socket is trusted, and filesystem permissions (mode `0600`,
owned by the user running the server) are the only ACL on the socket
itself. The proxy is responsible for *deciding* who gets to send
requests through to the socket.

## URL scheme

Remote clients talk to batchq with this URL form:

```
https://host[:port]/[subpath]
```

- **`https://` only.** Plain HTTP is not supported. There is no way to
  configure a remote client to use it — `support.ParseRemote` rejects
  any non-HTTPS scheme. Terminate TLS at your proxy.
- **Default port is 443.** Specify a port if you are running the proxy
  elsewhere.
- **The path is a mount-point prefix.** If your proxy serves batchq at
  `https://cluster.example.com/batchq`, set `remote = "https://cluster.example.com/batchq"`.
  The client appends `/api/v1/…` to every request, so the URL you
  configure is the prefix the API is mounted under, not the API URL
  itself.

## Configuring the client

Either in `$BATCHQ_HOME/config`:

```toml
[batchq]
remote = "https://cluster.example.com/batchq"
# token = ""              # shared token; prefer the BATCHQ_TOKEN env var
```

Or per-invocation:

```sh
batchq submit --remote https://cluster.example.com/batchq ./script.sh
```

When `remote` is set, the client:

- Dials the HTTPS URL directly — no unix socket lookup, no autospawn.
- Refuses to start a local `batchq server` (the server itself exits
  immediately if `[batchq] remote` is configured).

## Running the server behind a proxy

On the server host, run batchq normally. The server will create
`$BATCHQ_HOME/batchq.sock` with mode `0600`, owned by the user running
the server. The reverse proxy needs read+write access to that socket.

Two common ways to grant the proxy access:

1. **Run the proxy as the same user.** Simplest. Works on dedicated
   hosts. If you are running batchq as a system user `batchq`, run the
   proxy as `batchq` too.
2. **Use a group.** Run the server with `umask` set so the socket gets
   group write (`0660`), `chgrp` the socket to the proxy's group, and
   add the proxy to that group.

batchq does not currently offer a knob to set the socket mode or group
— it ships `0600`. If you need a wider mode, the cleanest approach is
to put the socket on a directory the proxy user has access to and run
the proxy as the same user.

### Listening on a TCP port (containers)

Sharing a unix socket between a batchq container and a proxy container
is awkward. For Docker / Kubernetes / any orchestrator, point
`[server] listen` (or `--listen`) at a TCP port instead:

```toml
[server]
listen = "tcp://0.0.0.0:8080"
token  = "a-long-random-secret"   # see below — strongly recommended
```

The proxy then forwards to `host:port` instead of a socket — in the
nginx example below, `server 127.0.0.1:8080;` in place of the
`unix:...` upstream.

A TCP port is plain HTTP and, unlike the unix socket, carries **no
kernel peer credentials** — so without a token the API is wide open to
anything that can reach the port. Always pair a TCP listener with either
`[server] token` (the server prints a startup warning if you don't) or a
proxy that authenticates, and keep the port on a trusted/internal network
(e.g. a Docker network, `127.0.0.1`, or a private subnet), never a public
interface without a proxy in front. batchq still never terminates TLS.

Inside a trusted network, other containers can talk to it directly
without a proxy by setting their own `[server] listen` (or
`BATCHQ_*`/config) to `tcp://<server-host>:8080` and the shared
`[batchq] token`; the client sends the token over the TCP connection.

## nginx example

```nginx
upstream batchq_socket {
    server unix:/var/lib/batchq/batchq.sock;
}

server {
    listen 443 ssl http2;
    server_name cluster.example.com;

    ssl_certificate     /etc/letsencrypt/live/cluster.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/cluster.example.com/privkey.pem;

    # Mount batchq at /batchq — clients use --remote https://cluster.example.com/batchq
    location /batchq/ {
        proxy_pass http://batchq_socket/;
        proxy_set_header Host $host;
        proxy_http_version 1.1;
        proxy_buffering off;
    }
}
```

Notes:

- The trailing slashes on both `location /batchq/` and the
  `proxy_pass` URL make nginx strip the `/batchq` prefix before
  forwarding. batchq does not see (or care about) the mount-point
  prefix; the client adds `/api/v1/...` directly.
- `proxy_buffering off` is a good idea for any streaming endpoints
  (log tails, future server-sent events).

To mount at the root instead, use `location /` and `proxy_pass
http://batchq_socket;` (no trailing slash on `proxy_pass`).

## Caddy example

```caddy
cluster.example.com {
    handle_path /batchq/* {
        reverse_proxy unix//var/lib/batchq/batchq.sock
    }
}
```

`handle_path` strips the matched prefix before forwarding, same as the
nginx setup above.

## Authentication — the shared token

Self-hosting your own network-exposed server is a **single-user,
secure-enough** path, not a multi-tenant one. The mechanism is a single
shared secret:

- On the **server**, set `[server] token` (or the `BATCHQ_SERVER_TOKEN`
  env var). The server then requires `Authorization: Bearer <token>` on
  every API request — a missing or wrong token gets a `401`. The health
  check (`/healthz`) is exempt so liveness probes and autospawn polling
  keep working.
- On every **client**, set the same value via `--token`,
  `BATCHQ_TOKEN`, or `[batchq] token`. The client sends it on every
  request, over HTTPS to a remote server and over the unix socket to a
  local one. That last part matters: a runner or CLI running on the
  *same host* as a token-protected server must also carry the token.

```toml
# On the server host ($BATCHQ_HOME/config), or via BATCHQ_SERVER_TOKEN:
[server]
token = "a-long-random-secret"

# On each client (and any local runner), or via BATCHQ_TOKEN:
[batchq]
remote = "https://cluster.example.com/batchq"
token  = "a-long-random-secret"
```

This is deliberately minimal. It is **one secret shared by everyone who
has it** — there is no per-user identity, no revocation granularity, and
no expiry. It closes the "anyone who can reach the proxy can drive the
queue" hole, which is the important one; it does not turn batchq into a
multi-tenant system. Treat the token like an SSH key: generate something
long and random, keep it in the env var rather than a checked-in file,
and rotate it by changing it on both ends.

> **Why only a shared token?** Real per-user authentication — tokens
> bound to an identity, validated server-side and fed into the
> hold/release/cancel authorization checks — belongs in the **managed**
> batchq server (the Postgres-backed, multi-tenant deployment), not in
> the single-file binary. The self-hosted server is meant to stay small.

### Defense in depth

The shared token is the in-band floor; it pairs well with gating at the
proxy, and for anything sensitive you should still do both:

- IP allow-lists on the proxy.
- A second authentication layer in front (mTLS, basic auth, an OAuth
  proxy, a corporate identity provider).
- VPN-only access to the proxy.

Do not expose a batchq server to the open internet on the strength of
the shared token alone.

### Over the unix socket

Without `[server] token`, the unix socket's `0600` permissions are the
only access control, and unix-socket clients get kernel-attested
identity from their peer credentials (see [running jobs](running-jobs.md)).
Setting `[server] token` adds the bearer-token requirement on **top** of
that for every request — so a local runner on a token-protected server
needs the token configured too.

## Operational notes

- **The server still autospawns.** If you run a reverse proxy and you
  are not supervising `batchq server`, the first request through the
  proxy will fail with a 502 because there is no server listening on
  the socket. For a remote-access deployment you almost always want a
  long-lived server: either start one with `batchq server` under a
  supervisor, or have something on the host trigger an autospawn
  periodically.
- **`--no-autospawn` for clients.** Remote clients never autospawn —
  the autospawn path only fires when the configured backend is the
  local unix socket. You do not need to pass `--no-autospawn` for
  remote clients; it is implicit.
- **The runner can be remote too.** `batchq run --slurm --remote
  https://…` is a perfectly fine setup. The runner connects to the
  remote batchq server for queue management and to the *local* SLURM
  via `sbatch`. You end up with a topology where the batchq server,
  the SLURM runner, and the SLURM head node can all be on different
  hosts.

## Where to go next

- [Architecture](architecture.md) — the why behind the socket-first
  transport and the autospawn model.
- [SLURM](slurm.md) — the SLURM runner often pairs with a remote
  batchq server on the cluster login node.
- [Web UI](web.md) — for browser access, SSH port-forwarding is often
  easier than a reverse proxy.
