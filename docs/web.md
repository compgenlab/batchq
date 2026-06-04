# Web UI

`batchq web` is a small read-only HTML interface to the queue. It is a
client of the batchq server like everything else — it does not have
its own copy of the database, and it does not modify jobs. The whole
thing is a thin renderer on top of the same REST API the CLI uses, so
what you see in the browser is exactly what `batchq queue` would show
you.

## Starting it

```sh
batchq web                                  # default: unix socket at
                                            # $BATCHQ_HOME/batchq-web.sock
batchq web --listen 127.0.0.1:8081          # TCP listener instead
batchq web --socket /tmp/batchq-web.sock    # custom socket path
batchq web --force                          # remove an existing socket first
batchq web -v                               # log every request to stderr
```

The web server starts and prints the URL it is listening on:

```
batchq web listening on unix:///home/me/.batchq/batchq-web.sock
```

You must configure exactly one of `--socket` and `--listen`. If both
are unset, the default unix socket path is used.

The web server keeps the batchq server alive while it is up — it pings
the batchq server every 30 seconds so an autospawned server with a
one-minute idle timeout never times out underneath the web UI.

## Connecting from another machine (SSH forward)

The easiest way to reach the web UI from your laptop, when batchq is
running on a remote host (cluster login node, shared server, …), is an
SSH local port forward. There are two shapes depending on whether the
web UI is listening on a unix socket or on TCP.

### When the web UI is on a unix socket (default)

```sh
ssh -L 8081:/home/me/.batchq/batchq-web.sock me@cluster.example.com
```

This binds `localhost:8081` on your laptop and forwards every byte to
the remote unix socket. Then open <http://localhost:8081/> in your
browser.

The `-L localport:/remote/socket/path` form requires a reasonably
modern OpenSSH on both ends (OpenSSH 6.7+ on the server, OpenSSH 8.1+
on the client to avoid quoting headaches with absolute paths).

### When the web UI is on a TCP listener

If you started `batchq web --listen 127.0.0.1:8081` on the remote host:

```sh
ssh -L 8081:127.0.0.1:8081 me@cluster.example.com
```

Then again open <http://localhost:8081/>.

### Keeping the forward up

For an ad-hoc session, the `ssh -L` above (without `-N`) is fine —
just leave the SSH window open. If you want it in the background:

```sh
ssh -fN -L 8081:127.0.0.1:8081 me@cluster.example.com
```

To make this convenient, drop the forward into your `~/.ssh/config`:

```
Host cluster
    HostName cluster.example.com
    User me
    LocalForward 8081 /home/me/.batchq/batchq-web.sock
```

Then `ssh cluster` opens both the shell and the forward.

### Multiple users on one host

If several people on the same login node are each running their own
`batchq web`, pick distinct socket paths or distinct ports — the unix
socket sits inside `$BATCHQ_HOME`, which is per-user by default, so
this usually takes care of itself.

## What you see

The UI has two pages:

### The queue page (`/`)

A table of jobs. By default it shows active jobs: `WAITING`, `QUEUED`,
`PROXYQUEUED`, `RUNNING`. Use the status checkboxes at the top to also
include `USERHOLD`, `SUCCESS`, `FAILED`, or `CANCELED`.

A search box and a "run id" box at the top let you narrow the listing:

- **Search** matches a substring across job id, name, script, and
  input/output paths. It searches every job, not just the currently
  visible ones.
- **Run id** filters to just the jobs sharing a specific `--run-id` —
  useful for looking at all the jobs in one workflow run together.

Array tasks carry an array column linking to the rest of their batch;
clicking it filters the listing to that array's tasks (the `?array_id=`
view). See [Job arrays](job-arrays.md).

Clicking a job id takes you to its detail page.

### The job detail page (`/jobs/{id}`)

Everything batchq knows about a single job, in one screen:

- **Header.** Job id, name, status (colored by status), and the basic
  resource request (procs / mem / walltime).
- **Details.** Every detail key/value pair on the job: working
  directory, stdout and stderr paths, the captured environment (just
  the count, not the values), input/output tags, run id.
- **Script.** The submitted shell script, monospace, in a scrolling
  panel.
- **Running details.** If the job has been claimed by a runner: which
  runner picked it up, start time, host, the SLURM job id (for jobs
  handed off to SLURM), and the generated sbatch script if applicable.
- **Logs.** Inline links to the stdout and stderr files. The link
  serves the last 256KB by default; append `?full=1` to the URL for
  the entire file. If the file does not exist yet (job hasn't started,
  or wasn't writing there), you see a placeholder with the path the
  runner expects.
- **Dependency tree.** If the job has `afterok` parents or dependents,
  a tree view of each. Click any node to navigate to that job's
  detail page.
- **Run forest.** If the job has a `--run-id`, a dependency-shaped
  view of every job sharing that run id. The current job is
  highlighted. This is the easiest way to see the shape of a multi-job
  pipeline.
- **Array tab.** If the job is a task of a job array, an "Array" tab
  with a per-status progress summary (how many of the array's tasks are
  queued / running / done / failed) and the list of sibling tasks, each
  linking to its own page. See [Job arrays](job-arrays.md).

## What you cannot do

The web UI is read-only. You cannot:

- Submit a job from it.
- Cancel, hold, release, or re-prioritise a job from it.
- Edit any field.

All of those are CLI-only today. The intent is that the web UI is for
*inspection* — you submit and manage from a shell, you watch and dig
into what is happening from a browser.

## Security notes

- The web UI has no authentication of its own. Anyone who can reach
  the socket or TCP listener can read everything in the queue,
  including script contents and (potentially sensitive) input/output
  paths.
- On a unix socket, the default mode is whatever batchq's umask
  produces — usually `0600`, owned by the user running `batchq web`.
  That means by default only the user themselves can read it, which is
  the correct posture for an SSH-port-forward setup.
- On a TCP listener, batchq web does **not** terminate TLS and does
  **not** authenticate. If you bind it to anything other than
  `127.0.0.1` you are exposing the queue contents to the network in
  the clear. Don't do that — put a reverse proxy in front. See
  [remote access](remote.md) for the same pattern applied to the API
  socket; the web UI's socket is treated the same way.

## Stopping the web UI

Send the process SIGINT or SIGTERM (Ctrl+C in the foreground). It
removes its socket file (if any) and exits cleanly.

## Where to go next

- [Submitting jobs](submitting-jobs.md) — what populates the queue
  the UI displays.
- [Job arrays](job-arrays.md) — the array column, `?array_id=` filter,
  and the job-page Array tab.
- [Configuration](configuration.md) — `[web]` knobs for the listener.
- [Remote access](remote.md) — for putting the API behind a reverse
  proxy; the same principles apply to the web socket.
