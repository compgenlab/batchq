# Installation

batchq is a single Go binary with no runtime dependencies. The whole
project is pure Go — `modernc.org/sqlite` is used in place of
`mattn/go-sqlite3`, so there is no CGO and no C cross-toolchain required
for any target.

## Requirements

- Go 1.23 or newer (only for building from source).
- Linux or macOS. The two supported platforms today are `linux` and
  `darwin` on `amd64` and `arm64`. Windows is not supported (the server
  binds a unix socket).
- A filesystem that supports unix sockets at `$BATCHQ_HOME`. Most
  network filesystems are fine for the *data* file but the *socket* must
  live on something local — see [architecture](architecture.md) for the
  full story.

## Building from source

```sh
git clone https://github.com/mbreese/batchq.git
cd batchq

# Build for the current host
go build -o bin/batchq main.go

# Or use the Makefile, which embeds the version string from `git describe`
make                                # bin/batchq.linux
make bin/batchq.macos_arm64         # cross-compile, no toolchain prep needed
make bin/batchq.linux_amd64
make bin/batchq.linux_arm64
```

Cross-compilation is just `GOOS=… GOARCH=… go build`; there is no extra
setup because there is no C toolchain in the build path.

The version string visible in `batchq version`, `batchq --version`, and
the help footer comes from `-ldflags '-X github.com/mbreese/batchq/cmd.Version=…'`
that the Makefile fills in from `git describe`. A plain `go build` with
no ldflags produces the literal string `"dev"`.

Install the binary somewhere on your `$PATH`:

```sh
install -m 0755 bin/batchq /usr/local/bin/batchq
```

## `$BATCHQ_HOME`

batchq stores its config file, the SQLite database, and its unix socket
in a single directory. By default this is `~/.batchq`. Override it by
setting the `BATCHQ_HOME` environment variable:

```sh
export BATCHQ_HOME=/var/lib/batchq
```

The directory will be created on first use if it does not exist.

The default layout is:

```
$BATCHQ_HOME/
  config             # TOML config file (see configuration.md)
  batchq.db          # SQLite database
  batchq.sock        # server's unix socket (mode 0600)
  batchq-web.sock    # web UI's unix socket
  spool/             # simple runner per-job temp dirs
```

Every one of these paths is overridable via the config file or
command-line flags — they are just the built-in defaults derived from
`$BATCHQ_HOME`.

## First run

You do not need to start the server yourself. Any CLI client
(`batchq submit`, `batchq queue`, …) will fork-exec a server with a short
idle timeout when it cannot reach the socket, then poll for the socket
to come up:

```sh
batchq submit ./hello.sh         # auto-spawns batchq server --idle-timeout 1m
batchq run                       # picks up and runs ./hello.sh
batchq queue                     # see the result
```

This is the intended mode of operation on every kind of deployment:
workstations, shared servers, and HPC login nodes. The server stays up
for a minute after the last request and then exits, so it never
becomes a permanent daemon — which matters because most HPC clusters
forbid those. Overlapping client requests in the same window share the
spawned server, which is what keeps the database safe on networked
filesystems.

If you specifically want a server that does not idle out, you can run
one in the foreground:

```sh
batchq server                    # foreground, no idle timeout
batchq server --idle-timeout 5m  # exit after 5 minutes of no requests
```

On a machine you own and where long-running user processes are
allowed, you can put `batchq server` under a service supervisor
(systemd, launchd, supervisord, runit) — but this is the exception,
not the default. A minimal systemd unit looks like:

```ini
[Unit]
Description=batchq server
After=network.target

[Service]
Type=simple
User=batchq
Environment=BATCHQ_HOME=/var/lib/batchq
ExecStart=/usr/local/bin/batchq server
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

Do not do this on a cluster login node unless you know your sysadmin
has agreed to it.

## Verifying the install

```sh
batchq version
batchq debug          # shows resolved config: $BATCHQ_HOME, config path,
                      # every env var consumed, every knob with its source
```

`batchq debug` is the canonical "what does my install actually think the
configuration is" command. Reach for it first when something is not
behaving the way you expected — it tells you whether a value came from
a flag, an environment variable, the config file, or the built-in
default.

## Uninstall

batchq has no persistent system state outside `$BATCHQ_HOME`. To remove
it completely:

```sh
batchq stop                  # shut down any running local server
rm /usr/local/bin/batchq
rm -rf "$BATCHQ_HOME"        # ~/.batchq by default
```

## Where to go next

- New to batchq? Read [overview](overview.md) and then
  [submitting jobs](submitting-jobs.md).
- Deploying to a cluster? See [SLURM](slurm.md) and
  [remote access](remote.md).
- Need to tune something? Every setting is documented in
  [configuration](configuration.md).
