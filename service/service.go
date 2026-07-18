// Package service is the server-side business logic for batchq. It sits
// between the REST handlers (in package server) and the persistence layer
// (in package storage). It owns:
//   - DTO ↔ storage conversion
//   - submission validation (assigning UUIDs, computing initial status)
//   - rate-limited dependency resolution
//   - composing state transitions (e.g. promote waiters after a job ends)
//
// The service has no knowledge of HTTP or transport.
package service

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/compgenlab/batchq/api"
	"github.com/compgenlab/batchq/jobs"
	"github.com/compgenlab/batchq/storage"
	"github.com/compgenlab/batchq/support"
)

// Errors mirrored at the service boundary so callers don't import storage
// directly to compare error values.
var (
	ErrJobNotFound  = storage.ErrJobNotFound
	ErrInvalidState = storage.ErrInvalidState
	ErrBadRequest   = errors.New("service: bad request")
)

// Service composes the storage layer with batchq's queue semantics.
type Service struct {
	store storage.Storage

	// archiveDir is where `cleanup --archive` writes archive DBs and where the
	// archive fallback lookup scans. Empty disables archiving/fallback.
	archiveDir string

	// cleanupRunning guards against concurrent bulk cleanups. Because a cleanup
	// decouples cancellation and can run for a while, a client that gives up and
	// retries must not launch a second one that contends with the first — so a
	// concurrent CleanupBulk is rejected rather than piled on.
	cleanupRunning atomic.Bool

	// resolveMu serializes dep-resolution work so a burst of state
	// transitions only triggers one ResolveDependencies pass.
	resolveMu sync.Mutex
}

// Option configures a Service at construction.
type Option func(*Service)

// WithArchiveDir sets the directory used for `cleanup --archive` and the archive
// fallback lookup. An empty dir leaves archiving disabled.
func WithArchiveDir(dir string) Option {
	return func(s *Service) { s.archiveDir = dir }
}

// New returns a Service backed by the given Storage.
func New(s storage.Storage, opts ...Option) *Service {
	svc := &Service{store: s}
	for _, o := range opts {
		o(svc)
	}
	return svc
}

// --- Submission --------------------------------------------------------

// SubmitJob persists a new job, assigning a UUID. Returns the persisted DTO.
//
// When the request arrived over a unix socket and the server captured
// peer credentials, this method overrides the uid/gid/groups details
// on the incoming request with the kernel-attested values resolved
// through NSS — the client cannot influence what identity the runner
// will use. When peer creds are absent (remote clients, tests with no
// ConnContext), the client-supplied uid/gid are preserved for
// backward compatibility; bearer-token-derived identity will fill
// that gap in a later change.
func (s *Service) SubmitJob(ctx context.Context, req *api.SubmitJobRequest) (*api.JobDTO, error) {
	if req == nil {
		return nil, ErrBadRequest
	}
	if _, ok := req.Details["script"]; !ok {
		return nil, fmt.Errorf("%w: missing details.script", ErrBadRequest)
	}

	if peer, ok := support.PeerCredsFromContext(ctx); ok {
		applyPeerIdentity(req, peer)
	}

	afterOk := append([]string{}, req.AfterOk...)
	if len(req.ArrayDeps) > 0 {
		specs, err := parseDepSpecs(req.ArrayDeps)
		if err != nil {
			return nil, err
		}
		extra, err := s.expandSingleDeps(ctx, specs)
		if err != nil {
			return nil, err
		}
		afterOk = append(afterOk, extra...)
	}

	job := &jobs.JobDef{
		JobId:       support.NewUUID(),
		Name:        jobs.NormalizeName(req.Name),
		Notes:       req.Notes,
		Priority:    req.Priority,
		AfterOk:     afterOk,
		InputFiles:  req.InputFiles,
		OutputFiles: req.OutputFiles,
	}
	if req.Hold {
		job.Status = jobs.USERHOLD
	}
	for k, v := range req.Details {
		job.Details = append(job.Details, jobs.JobDefDetail{Key: k, Value: v})
	}

	if err := s.store.InsertJob(ctx, job); err != nil {
		return nil, err
	}
	return api.JobFromDef(job), nil
}

type depSpec struct {
	kind   string // "afterok" | "aftercorr"
	target string // a job id or an array id
}

type arrayMember struct {
	id    string
	index int
}

// parseDepSpecs parses "kind:target" dependency entries.
func parseDepSpecs(arrayDeps []string) ([]depSpec, error) {
	var out []depSpec
	for _, d := range arrayDeps {
		i := strings.Index(d, ":")
		if i < 0 {
			return nil, fmt.Errorf("%w: malformed dependency %q", ErrBadRequest, d)
		}
		kind, target := d[:i], d[i+1:]
		if target == "" || (kind != "afterok" && kind != "aftercorr") {
			return nil, fmt.Errorf("%w: malformed dependency %q", ErrBadRequest, d)
		}
		out = append(out, depSpec{kind: kind, target: target})
	}
	return out, nil
}

// loadArrayMembers returns the member jobs of an array id, or nil if the id is
// not an array (i.e. a plain job id).
func (s *Service) loadArrayMembers(ctx context.Context, arrayID string) ([]arrayMember, error) {
	rows, err := s.store.FindArrayMembers(ctx, arrayID)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	members := make([]arrayMember, 0, len(rows))
	for _, r := range rows {
		members = append(members, arrayMember{id: r.ID, index: r.Index})
	}
	return members, nil
}

// resolveDepTarget resolves a dependency spec's target string. A task address
// "<array_id>_<index>" resolves to that one task's job id (returned as singleID);
// an array id returns its members; a plain job id returns (nil, "", nil) and the
// caller uses the target verbatim. An unknown array or missing task index is a
// bad request.
func (s *Service) resolveDepTarget(ctx context.Context, target string) (members []arrayMember, singleID string, err error) {
	if arrayID, idxStr, ok := support.SplitTaskAddr(target); ok {
		ms, err := s.loadArrayMembers(ctx, arrayID)
		if err != nil {
			return nil, "", err
		}
		if ms == nil {
			return nil, "", fmt.Errorf("%w: dependency %q references unknown array %s", ErrBadRequest, target, arrayID)
		}
		want, _ := strconv.Atoi(idxStr)
		for _, m := range ms {
			if m.index == want {
				return nil, m.id, nil
			}
		}
		return nil, "", fmt.Errorf("%w: dependency %q: array %s has no task %s", ErrBadRequest, target, arrayID, idxStr)
	}
	ms, err := s.loadArrayMembers(ctx, target)
	return ms, "", err
}

// expandSingleDeps resolves dependency specs for a single (non-array) dependent.
// afterok on an array fans out to all its members; a task address resolves to one
// member; aftercorr is invalid.
func (s *Service) expandSingleDeps(ctx context.Context, specs []depSpec) ([]string, error) {
	var afterok []string
	for _, sp := range specs {
		if sp.kind == "aftercorr" {
			return nil, fmt.Errorf("%w: aftercorr requires an array dependent", ErrBadRequest)
		}
		members, singleID, err := s.resolveDepTarget(ctx, sp.target)
		if err != nil {
			return nil, err
		}
		switch {
		case singleID != "":
			afterok = append(afterok, singleID)
		case members == nil:
			afterok = append(afterok, sp.target)
		default:
			for _, m := range members {
				afterok = append(afterok, m.id)
			}
		}
	}
	return afterok, nil
}

// expandArrayDepsForTasks resolves dependency specs into per-array-index afterok
// edges. afterok fans out to every task; aftercorr pairs task i with the dep
// array's task whose index equals i (requiring matching index sets).
func (s *Service) expandArrayDepsForTasks(ctx context.Context, specs []depSpec, indices []int) (map[int][]string, error) {
	cache := map[string][]arrayMember{}
	loaded := map[string]bool{}
	load := func(target string) ([]arrayMember, error) {
		if loaded[target] {
			return cache[target], nil
		}
		m, err := s.loadArrayMembers(ctx, target)
		if err != nil {
			return nil, err
		}
		cache[target] = m
		loaded[target] = true
		return m, nil
	}

	perTask := map[int][]string{}
	for _, sp := range specs {
		// A task address "<array_id>_<index>" targets one specific task. For
		// afterok every dependent task waits on that one task; aftercorr needs a
		// whole array to pair index-wise, so a task address is invalid there.
		if arrayID, idxStr, ok := support.SplitTaskAddr(sp.target); ok {
			if sp.kind == "aftercorr" {
				return nil, fmt.Errorf("%w: aftercorr target %s must be an array, not a single task", ErrBadRequest, sp.target)
			}
			ms, err := load(arrayID)
			if err != nil {
				return nil, err
			}
			if ms == nil {
				return nil, fmt.Errorf("%w: dependency %q references unknown array %s", ErrBadRequest, sp.target, arrayID)
			}
			want, _ := strconv.Atoi(idxStr)
			depID := ""
			for _, m := range ms {
				if m.index == want {
					depID = m.id
					break
				}
			}
			if depID == "" {
				return nil, fmt.Errorf("%w: dependency %q: array %s has no task %s", ErrBadRequest, sp.target, arrayID, idxStr)
			}
			for _, idx := range indices {
				perTask[idx] = append(perTask[idx], depID)
			}
			continue
		}
		members, err := load(sp.target)
		if err != nil {
			return nil, err
		}
		switch sp.kind {
		case "afterok":
			var ids []string
			if members == nil {
				ids = []string{sp.target}
			} else {
				for _, m := range members {
					ids = append(ids, m.id)
				}
			}
			for _, idx := range indices {
				perTask[idx] = append(perTask[idx], ids...)
			}
		case "aftercorr":
			if members == nil {
				return nil, fmt.Errorf("%w: aftercorr target %s is not an array", ErrBadRequest, sp.target)
			}
			if len(members) != len(indices) {
				return nil, fmt.Errorf("%w: aftercorr size mismatch (dep array has %d tasks, this array has %d)", ErrBadRequest, len(members), len(indices))
			}
			byIndex := make(map[int]string, len(members))
			for _, m := range members {
				byIndex[m.index] = m.id
			}
			for _, idx := range indices {
				dep, ok := byIndex[idx]
				if !ok {
					return nil, fmt.Errorf("%w: aftercorr index %d has no matching task in the dependency array", ErrBadRequest, idx)
				}
				perTask[idx] = append(perTask[idx], dep)
			}
		}
	}
	return perTask, nil
}

// SubmitArray expands one job template into N task-jobs (one per array index),
// all sharing a generated array_id, and persists them atomically. Each task
// carries array_id/array_index/array_size (and array_throttle when set) details
// in addition to the shared template details/resources.
func (s *Service) SubmitArray(ctx context.Context, req *api.SubmitArrayRequest) (*api.SubmitArrayResponse, error) {
	if req == nil {
		return nil, ErrBadRequest
	}
	if _, ok := req.Details["script"]; !ok {
		return nil, fmt.Errorf("%w: missing details.script", ErrBadRequest)
	}
	if len(req.ArrayIndices) == 0 {
		return nil, fmt.Errorf("%w: empty array", ErrBadRequest)
	}

	// Apply peer identity once to the shared template before fanning out.
	if peer, ok := support.PeerCredsFromContext(ctx); ok {
		applyPeerIdentity(&req.SubmitJobRequest, peer)
	}

	// Resolve array-aware dependencies into per-task afterok edges.
	var perTaskDeps map[int][]string
	if len(req.ArrayDeps) > 0 {
		specs, err := parseDepSpecs(req.ArrayDeps)
		if err != nil {
			return nil, err
		}
		perTaskDeps, err = s.expandArrayDepsForTasks(ctx, specs, req.ArrayIndices)
		if err != nil {
			return nil, err
		}
	}

	arrayID := support.NewUUID()
	size := strconv.Itoa(len(req.ArrayIndices))
	throttle := ""
	if req.ArrayThrottle > 0 {
		throttle = strconv.Itoa(req.ArrayThrottle)
	}

	tasks := make([]*jobs.JobDef, 0, len(req.ArrayIndices))
	for _, idx := range req.ArrayIndices {
		afterOk := append([]string{}, req.AfterOk...)
		afterOk = append(afterOk, perTaskDeps[idx]...)
		task := &jobs.JobDef{
			JobId:       support.NewUUID(),
			Name:        jobs.NormalizeName(req.Name),
			Notes:       req.Notes,
			Priority:    req.Priority,
			AfterOk:     afterOk,
			InputFiles:  req.InputFiles,
			OutputFiles: req.OutputFiles,
		}
		if req.Hold {
			task.Status = jobs.USERHOLD
		}
		for k, v := range req.Details {
			task.Details = append(task.Details, jobs.JobDefDetail{Key: k, Value: v})
		}
		task.Details = append(task.Details,
			jobs.JobDefDetail{Key: "array_id", Value: arrayID},
			jobs.JobDefDetail{Key: "array_index", Value: strconv.Itoa(idx)},
			jobs.JobDefDetail{Key: "array_size", Value: size},
		)
		if throttle != "" {
			task.Details = append(task.Details, jobs.JobDefDetail{Key: "array_throttle", Value: throttle})
		}
		tasks = append(tasks, task)
	}

	if err := s.store.InsertArray(ctx, arrayID, tasks); err != nil {
		return nil, err
	}

	resp := &api.SubmitArrayResponse{ArrayID: arrayID}
	for _, t := range tasks {
		resp.JobIDs = append(resp.JobIDs, t.JobId)
		resp.Jobs = append(resp.Jobs, api.JobFromDef(t))
	}
	return resp, nil
}

// applyPeerIdentity overwrites the uid/gid/groups details on req with
// the kernel-attested values from peer plus the user's supplementary
// groups looked up via NSS. If the NSS lookup fails, the peer's
// uid/gid are still written (no spoofing possible) but the groups
// detail is left as whatever the client sent, falling back to "no
// supplementary groups" if the client sent none.
func applyPeerIdentity(req *api.SubmitJobRequest, peer support.PeerCreds) {
	if req.Details == nil {
		req.Details = map[string]string{}
	}
	req.Details["uid"] = strconv.FormatUint(uint64(peer.Uid), 10)
	req.Details["gid"] = strconv.FormatUint(uint64(peer.Gid), 10)

	ident, err := support.LookupUserByUid(peer.Uid)
	if err != nil {
		// NSS resolved partial info (uid known, groups failed): keep
		// the primary identity overrides we already wrote and leave
		// groups detail alone. If NSS doesn't know the uid at all
		// (ErrUserNotFound), same story — uid/gid are still
		// kernel-attested, supp groups just won't be set.
		if !errors.Is(err, support.ErrUserNotFound) {
			log.Printf("service: NSS lookup for uid %d: %v", peer.Uid, err)
		}
		if ident.Username != "" && ident.Gid != 0 {
			req.Details["gid"] = strconv.FormatUint(uint64(ident.Gid), 10)
		}
		return
	}
	// Full lookup succeeded; trust NSS's primary gid over the peer's
	// (the peer's gid is the user's gid at connect time, which equals
	// the NSS primary on a normal login but can be overridden by
	// newgrp/setgid binaries; NSS is canonical).
	req.Details["gid"] = strconv.FormatUint(uint64(ident.Gid), 10)
	if len(ident.Groups) > 0 {
		req.Details["groups"] = joinUint32(ident.Groups, ",")
	} else {
		delete(req.Details, "groups")
	}
}

// joinUint32 formats a slice of uint32 as a sep-separated string,
// without allocating a separate []string. Used for the "groups"
// detail wire format.
func joinUint32(vals []uint32, sep string) string {
	if len(vals) == 0 {
		return ""
	}
	var b strings.Builder
	for i, v := range vals {
		if i > 0 {
			b.WriteString(sep)
		}
		b.WriteString(strconv.FormatUint(uint64(v), 10))
	}
	return b.String()
}

// --- Reads -------------------------------------------------------------

func (s *Service) GetJob(ctx context.Context, jobID string, includeArchives bool) (*api.JobDTO, error) {
	job, err := s.store.GetJob(ctx, jobID)
	if err == nil {
		return api.JobFromDef(job), nil
	}
	// Fallback: consult archive DBs only on a live miss, and only when asked.
	if includeArchives && errors.Is(err, storage.ErrJobNotFound) {
		if aj, aerr := s.getArchivedJob(ctx, jobID); aerr == nil {
			return api.JobFromDef(aj), nil
		}
	}
	return nil, err
}

// ListJobsOptions controls the GET /jobs endpoint.
type ListJobsOptions struct {
	ShowAll      bool
	SortByStatus bool
	Statuses     []jobs.StatusCode
	Query        string

	// RunID, ArrayID, Output, and Input are optional intersect-filters
	// applied after the base listing. Empty values are ignored.
	RunID   string
	ArrayID string
	Output  string
	Input   string

	// Since and Before optionally bound the base listing by submit time:
	// keep jobs with SubmitTime >= Since and SubmitTime < Before. Zero
	// values are ignored.
	Since  time.Time
	Before time.Time

	// IncludeArchives unions matches from the archive DBs into the result.
	// Opt-in ("within reason") — archives are only opened when this is set.
	IncludeArchives bool
}

func (s *Service) ListJobs(ctx context.Context, opts ListJobsOptions) ([]*api.JobDTO, error) {
	out, err := s.listFrom(ctx, s.store, opts)
	if err != nil {
		return nil, err
	}
	if opts.IncludeArchives {
		paths, perr := s.archivePaths()
		if perr != nil {
			return nil, perr
		}
		seen := make(map[string]struct{}, len(out))
		for _, j := range out {
			seen[j.JobId] = struct{}{}
		}
		for _, p := range paths {
			arc, oerr := storage.OpenReadOnly(ctx, p)
			if oerr != nil {
				continue // skip an unreadable archive rather than failing the whole query
			}
			ajobs, lerr := s.listFrom(ctx, arc, opts)
			arc.Close()
			if lerr != nil {
				continue
			}
			for _, j := range ajobs {
				if _, dup := seen[j.JobId]; dup {
					continue
				}
				seen[j.JobId] = struct{}{}
				out = append(out, j)
			}
		}
	}
	return toDTOs(out), nil
}

// listFrom runs the ListJobs computation against a specific store (the live
// store, or an archive DB during a fallback lookup). Returns hydrated JobDefs.
func (s *Service) listFrom(ctx context.Context, store storage.Storage, opts ListJobsOptions) ([]*jobs.JobDef, error) {
	// A detail filter (array/run/output/input) narrows the base listing to a
	// small set. Skip per-job relation hydration in the base query and hydrate
	// only the survivors — otherwise listing e.g. one array's tasks would
	// N+1-hydrate every job in the database first, then throw most away.
	detailFilter := opts.RunID != "" || opts.ArrayID != "" || opts.Output != "" || opts.Input != ""
	loadRelations := !detailFilter

	var (
		out []*jobs.JobDef
		err error
	)
	switch {
	case opts.Query != "":
		out, err = store.SearchJobs(ctx, opts.Query, opts.Statuses, opts.Since, opts.Before, loadRelations)
	case len(opts.Statuses) > 0:
		out, err = store.ListJobsByStatus(ctx, opts.Statuses, opts.SortByStatus, opts.Since, opts.Before, loadRelations)
	default:
		out, err = store.ListJobs(ctx, opts.ShowAll, opts.SortByStatus, opts.Since, opts.Before, loadRelations)
	}
	if err != nil {
		return nil, err
	}

	if detailFilter {
		var allow map[string]struct{}
		if opts.RunID != "" {
			ids, err := store.FindJobsByDetail(ctx, "run_id", opts.RunID)
			if err != nil {
				return nil, err
			}
			allow = intersect(allow, ids)
		}
		if opts.ArrayID != "" {
			ids, err := store.FindJobsByDetail(ctx, "array_id", opts.ArrayID)
			if err != nil {
				return nil, err
			}
			allow = intersect(allow, ids)
		}
		if opts.Output != "" {
			ids, err := store.FindJobsByOutputPath(ctx, opts.Output)
			if err != nil {
				return nil, err
			}
			allow = intersect(allow, ids)
		}
		if opts.Input != "" {
			ids, err := store.FindJobsByInputPath(ctx, opts.Input)
			if err != nil {
				return nil, err
			}
			allow = intersect(allow, ids)
		}
		filtered := make([]*jobs.JobDef, 0, len(out))
		for _, j := range out {
			if _, ok := allow[j.JobId]; ok {
				filtered = append(filtered, j)
			}
		}
		out = filtered

		// The base listing ran with loadRelations=false; hydrate only the
		// jobs that survived the detail filter.
		if err := store.HydrateJobs(ctx, out); err != nil {
			return nil, err
		}
	}

	// since/before are applied in SQL by the store methods above, not here.
	return out, nil
}

// intersect merges a new set of IDs into the running allow-set. The
// first call (allow == nil) seeds it with ids; subsequent calls keep
// only IDs present in both.
func intersect(allow map[string]struct{}, ids []string) map[string]struct{} {
	if allow == nil {
		out := make(map[string]struct{}, len(ids))
		for _, id := range ids {
			out[id] = struct{}{}
		}
		return out
	}
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		seen[id] = struct{}{}
	}
	out := make(map[string]struct{})
	for id := range allow {
		if _, ok := seen[id]; ok {
			out[id] = struct{}{}
		}
	}
	return out
}

func (s *Service) GetQueueJobs(ctx context.Context, showAll, sortByStatus bool, since, before time.Time) ([]*api.JobDTO, error) {
	out, err := s.store.GetQueueJobs(ctx, showAll, sortByStatus, since, before)
	if err != nil {
		return nil, err
	}
	return toDTOs(out), nil
}

func (s *Service) GetJobDependents(ctx context.Context, jobID string) ([]string, error) {
	return s.store.GetJobDependents(ctx, jobID)
}

func (s *Service) GetJobStatusCounts(ctx context.Context, showAll bool) (map[string]int, error) {
	counts, err := s.store.GetJobStatusCounts(ctx, showAll)
	if err != nil {
		return nil, err
	}
	out := make(map[string]int, len(counts))
	for k, v := range counts {
		out[k.String()] = v
	}
	return out, nil
}

func toDTOs(in []*jobs.JobDef) []*api.JobDTO {
	out := make([]*api.JobDTO, 0, len(in))
	for _, j := range in {
		out = append(out, api.JobFromDef(j))
	}
	return out
}

// --- User actions ------------------------------------------------------

// ErrForbidden is returned by user-action methods when the caller is
// authenticated (via peer creds) but does not own the target job and
// is not root.
var ErrForbidden = errors.New("service: forbidden")

func (s *Service) CancelJob(ctx context.Context, jobID, reason string) error {
	if err := s.authorizeJobAction(ctx, jobID); err != nil {
		return err
	}
	if reason == "" {
		reason = "user request"
	}
	return s.store.CancelJob(ctx, jobID, reason)
}

func (s *Service) HoldJob(ctx context.Context, jobID string) error {
	if err := s.authorizeJobAction(ctx, jobID); err != nil {
		return err
	}
	return s.store.HoldJob(ctx, jobID)
}

func (s *Service) ReleaseJob(ctx context.Context, jobID string) error {
	if err := s.authorizeJobAction(ctx, jobID); err != nil {
		return err
	}
	if err := s.store.ReleaseJob(ctx, jobID); err != nil {
		return err
	}
	// A just-released job may now be eligible to QUEUE; trigger a pass.
	_, _ = s.ResolveDependencies(ctx)
	return nil
}

func (s *Service) CancelArray(ctx context.Context, arrayID, reason string) (int, error) {
	if err := s.authorizeArrayAction(ctx, arrayID); err != nil {
		return 0, err
	}
	if reason == "" {
		reason = "user request"
	}
	return s.store.CancelArray(ctx, arrayID, reason)
}

func (s *Service) HoldArray(ctx context.Context, arrayID string) (int, error) {
	if err := s.authorizeArrayAction(ctx, arrayID); err != nil {
		return 0, err
	}
	return s.store.HoldArray(ctx, arrayID)
}

func (s *Service) ReleaseArray(ctx context.Context, arrayID string) (int, error) {
	if err := s.authorizeArrayAction(ctx, arrayID); err != nil {
		return 0, err
	}
	n, err := s.store.ReleaseArray(ctx, arrayID)
	if err != nil {
		return 0, err
	}
	// Just-released tasks may now be eligible to QUEUE; trigger a pass.
	_, _ = s.ResolveDependencies(ctx)
	return n, nil
}

// authorizeArrayAction authorizes an array operation via its first member
// (tasks of an array share an owner). Errors if the array has no tasks.
func (s *Service) authorizeArrayAction(ctx context.Context, arrayID string) error {
	ids, err := s.store.FindJobsByDetail(ctx, "array_id", arrayID)
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		return fmt.Errorf("%w: no such array %s", ErrBadRequest, arrayID)
	}
	return s.authorizeJobAction(ctx, ids[0])
}

// authorizeJobAction enforces the per-job operator check: a caller
// with kernel-attested peer credentials (i.e. a unix-socket client)
// can only act on jobs whose stored uid matches their own. Root
// (uid 0) is an admin and can act on any job. Requests without peer
// credentials (in-process tests, remote/proxy clients arriving via
// HTTP through a future TCP listener) are allowed through; remote
// authz will move under the bearer-token mechanism in a later change.
func (s *Service) authorizeJobAction(ctx context.Context, jobID string) error {
	peer, ok := support.PeerCredsFromContext(ctx)
	if !ok {
		return nil
	}
	if peer.Uid == 0 {
		return nil
	}
	job, err := s.store.GetJob(ctx, jobID)
	if err != nil {
		return err
	}
	for _, d := range job.Details {
		if d.Key != "uid" {
			continue
		}
		ownerUid, perr := strconv.ParseUint(strings.TrimSpace(d.Value), 10, 32)
		if perr != nil {
			// A job with an unparseable uid detail is older or
			// corrupt; fail-closed for safety — an operator can
			// always use root to intervene.
			return fmt.Errorf("%w: job %s has unparseable uid detail", ErrForbidden, jobID)
		}
		if uint32(ownerUid) == peer.Uid {
			return nil
		}
		return fmt.Errorf("%w: job %s belongs to uid %d", ErrForbidden, jobID, ownerUid)
	}
	// No uid detail at all (older pre-identity job): same fail-closed
	// posture. Root remains the escape hatch.
	return fmt.Errorf("%w: job %s has no uid detail", ErrForbidden, jobID)
}

func (s *Service) AdjustJobPriority(ctx context.Context, jobID string, delta int) error {
	if delta == 0 {
		return nil
	}
	return s.store.AdjustJobPriority(ctx, jobID, delta)
}

// AdjustArrayPriority applies delta to every eligible task of an array in one
// statement. Returns the number of tasks adjusted.
func (s *Service) AdjustArrayPriority(ctx context.Context, arrayID string, delta int) (int, error) {
	if err := s.authorizeArrayAction(ctx, arrayID); err != nil {
		return 0, err
	}
	if delta == 0 {
		return 0, nil
	}
	return s.store.AdjustArrayPriority(ctx, arrayID, delta)
}

func (s *Service) CleanupJob(ctx context.Context, jobID string) error {
	job, err := s.store.GetJob(ctx, jobID)
	if err != nil {
		return err
	}
	if !isTerminal(job.Status) {
		return fmt.Errorf("%w: cannot clean up non-terminal job (%s)", ErrInvalidState, job.Status)
	}
	return s.store.CleanupJob(ctx, jobID)
}

func isTerminal(s jobs.StatusCode) bool {
	return s == jobs.SUCCESS || s == jobs.FAILED || s == jobs.CANCELED
}

// Vacuum reclaims free pages in the live DB (VACUUM). Run after a purge to
// actually shrink the file. May be slow on a large DB — callers budget a long
// timeout (see the long-running client in cmd/).
func (s *Service) Vacuum(ctx context.Context) error {
	return s.store.Vacuum(ctx)
}

// ArchiveJobs moves the given terminal jobs into a NEW archive DB under the
// server's archive dir, then makes that file read-only. ids must be in the
// cleanup planner's dependency-safe order (dependents before parents) so the
// per-job delete-from-live satisfies foreign keys. archiveName selects the file
// (<archiveDir>/<name>.db); empty picks a timestamped default. The archive must
// not already exist — archives are write-once. Each job is copied into the
// archive and committed there BEFORE it is deleted from the live DB, so a crash
// mid-run never loses a job (it stays live and re-archives cleanly). Returns the
// archive path and the number of jobs archived.
func (s *Service) ArchiveJobs(ctx context.Context, ids []string, archiveName string) (string, int, error) {
	return s.archiveOrdered(ctx, ids, archiveName, nil)
}

// archiveOrdered is ArchiveJobs with an optional per-batch progress callback
// (cumulative jobs archived). Used by CleanupBulk to stream progress.
func (s *Service) archiveOrdered(ctx context.Context, ids []string, archiveName string, onProgress func(done int)) (string, int, error) {
	if s.archiveDir == "" {
		return "", 0, fmt.Errorf("%w: no archive_dir configured", ErrBadRequest)
	}
	name := sanitizeArchiveName(archiveName)
	if name == "" {
		name = "archive-" + time.Now().Format("20060102-150405")
	}
	if err := os.MkdirAll(s.archiveDir, 0o755); err != nil {
		return "", 0, fmt.Errorf("archive: create dir: %w", err)
	}
	path := filepath.Join(s.archiveDir, name+".db")
	if _, err := os.Stat(path); err == nil {
		return "", 0, fmt.Errorf("%w: archive already exists: %s", ErrBadRequest, path)
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", 0, fmt.Errorf("archive: stat destination: %w", err)
	}

	arc, err := storage.OpenArchiveWriter(ctx, path)
	if err != nil {
		return "", 0, fmt.Errorf("archive: open writer: %w", err)
	}

	// Bulk move, one chunk at a time: bulk-read the chunk, bulk-write it to the
	// archive (one fsync), then bulk-delete it from live — vs the old per-job
	// path's ~18 fsync'd statements per job over NFS. ids are in the planner's
	// dependency-safe post-order (dependents before parents), so each chunk's
	// dependents are all in this-or-earlier (already-deleted) chunks, keeping the
	// per-chunk delete foreign-key-safe. Archive-then-delete per chunk stays
	// crash-safe (a job is committed to the archive before it leaves live).
	const chunkSize = 500
	count := 0
	var moveErr error
	for start := 0; start < len(ids); start += chunkSize {
		// Cancelable between chunks (not mid-transaction): if the client
		// disconnected / the op was aborted, stop cleanly here rather than
		// orphaning a multi-hour run.
		if err := ctx.Err(); err != nil {
			moveErr = err
			break
		}
		end := start + chunkSize
		if end > len(ids) {
			end = len(ids)
		}
		chunk := ids[start:end]

		batch, err := s.store.LoadJobs(ctx, chunk)
		if err != nil {
			moveErr = fmt.Errorf("archive: load batch: %w", err)
			break
		}
		for _, j := range batch {
			if !isTerminal(j.Status) {
				moveErr = fmt.Errorf("%w: cannot archive non-terminal job %s (%s)", ErrInvalidState, j.JobId, j.Status)
				break
			}
		}
		if moveErr != nil {
			break
		}
		if err := arc.RestoreJobs(ctx, batch); err != nil {
			moveErr = fmt.Errorf("archive: write batch: %w", err)
			break
		}
		if err := s.store.CleanupJobs(ctx, chunk, nil); err != nil {
			moveErr = fmt.Errorf("archive: remove batch from live: %w", err)
			break
		}
		count += len(batch)
		if onProgress != nil {
			onProgress(count)
		}
	}
	_ = arc.Close()

	if count == 0 {
		// Nothing archived — drop the empty file so no read-only husk lingers.
		_ = os.Remove(path)
		return path, 0, moveErr
	}
	// Finalize: archives are write-once. Read-only enforces immutability and lets
	// the fallback lookup open them without contending for write access.
	if err := os.Chmod(path, 0o440); err != nil && moveErr == nil {
		moveErr = fmt.Errorf("archive: set read-only: %w", err)
	}
	return path, count, moveErr
}

// sanitizeArchiveName strips any directory components and a trailing .db from a
// user-supplied archive name, so `--archive foo` and `--archive foo.db` both map
// to <archiveDir>/foo.db and a name can never escape the archive dir.
func sanitizeArchiveName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return ""
	}
	name = filepath.Base(name)
	name = strings.TrimSuffix(name, ".db")
	if name == "." || name == ".." || name == "/" {
		return ""
	}
	return name
}

// archivePaths lists the archive DB files (*.db) in the archive dir, newest
// first (timestamped names sort chronologically, so reverse-lexical ≈ newest
// first). Empty when no archive dir is configured or it doesn't exist.
func (s *Service) archivePaths() ([]string, error) {
	if s.archiveDir == "" {
		return nil, nil
	}
	entries, err := os.ReadDir(s.archiveDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var paths []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".db") {
			continue
		}
		paths = append(paths, filepath.Join(s.archiveDir, e.Name()))
	}
	sort.Sort(sort.Reverse(sort.StringSlice(paths)))
	return paths, nil
}

// getArchivedJob scans archive DBs for a job by id, returning the first hit.
// Returns ErrJobNotFound when no archive has it.
func (s *Service) getArchivedJob(ctx context.Context, jobID string) (*jobs.JobDef, error) {
	paths, err := s.archivePaths()
	if err != nil {
		return nil, err
	}
	for _, p := range paths {
		arc, oerr := storage.OpenReadOnly(ctx, p)
		if oerr != nil {
			continue // skip an unreadable archive
		}
		job, gerr := arc.GetJob(ctx, jobID)
		arc.Close()
		if gerr == nil {
			return job, nil
		}
	}
	return nil, storage.ErrJobNotFound
}

// CleanupBulkOptions parameterizes a server-side bulk cleanup.
type CleanupBulkOptions struct {
	Statuses    []jobs.StatusCode // terminal statuses to select; empty = purge nothing
	OlderThan   time.Duration     // only jobs whose end_time is older than this; 0 = no age filter
	Archive     bool              // true: archive-then-delete; false: delete
	ArchiveName string            // archive file name (Archive only); empty = timestamped
	Vacuum      bool              // VACUUM the live DB after the purge
}

// CleanupBulkResult reports what a bulk cleanup did.
type CleanupBulkResult struct {
	Removed     int    // jobs deleted (or archived+deleted)
	Blocked     int    // eligible jobs left in place because a dependent wasn't eligible
	ArchivePath string // server-side archive file, when Archive
	Vacuumed    bool
}

// CleanupBulk selects eligible terminal jobs (by status + age, in SQL), computes
// a dependency-safe removal order in-process from bulk queries (no per-job
// round-trips), then deletes — or archives-then-deletes — them, optionally
// vacuuming afterward. This is the scalable replacement for the old client-side
// planner: the whole operation is one server call over data next to the DB.
//
// progress (may be nil) receives an event per phase — the candidate selection,
// per-batch delete/archive progress, and the vacuum — so a caller (the streaming
// handler) can report live on a long-running purge. The terminal "done"/"error"
// event is emitted by the caller, not here.
func (s *Service) CleanupBulk(ctx context.Context, opts CleanupBulkOptions, progress func(api.CleanupEvent)) (CleanupBulkResult, error) {
	var res CleanupBulkResult

	// Reject a concurrent cleanup (see cleanupRunning) instead of contending.
	if !s.cleanupRunning.CompareAndSwap(false, true) {
		return res, fmt.Errorf("%w: a cleanup is already in progress", ErrInvalidState)
	}
	defer s.cleanupRunning.Store(false)

	emit := func(ev api.CleanupEvent) {
		if progress != nil {
			progress(ev)
		}
	}

	if len(opts.Statuses) > 0 {
		var endBefore time.Time
		if opts.OlderThan > 0 {
			endBefore = time.Now().Add(-opts.OlderThan)
		}
		ids, edges, err := s.store.CleanupCandidates(ctx, opts.Statuses, endBefore)
		if err != nil {
			return res, err
		}
		order, blocked := planRemoval(ids, edges)
		res.Blocked = blocked
		emit(api.CleanupEvent{Phase: api.CleanupPhaseSelected, Matched: len(ids), Total: len(order), Blocked: blocked})

		if len(order) > 0 {
			if opts.Archive {
				path, count, err := s.archiveOrdered(ctx, order, opts.ArchiveName, func(done int) {
					emit(api.CleanupEvent{Phase: api.CleanupPhaseArchiving, Done: done, Total: len(order)})
				})
				if err != nil {
					return res, err
				}
				res.ArchivePath = path
				res.Removed = count
			} else {
				if err := s.store.CleanupJobs(ctx, order, func(done int) {
					emit(api.CleanupEvent{Phase: api.CleanupPhaseDeleting, Done: done, Total: len(order)})
				}); err != nil {
					return res, err
				}
				res.Removed = len(order)
			}
		}
	}

	if opts.Vacuum {
		emit(api.CleanupEvent{Phase: api.CleanupPhaseVacuum})
		if err := s.store.Vacuum(ctx); err != nil {
			return res, err
		}
		res.Vacuumed = true
	}
	return res, nil
}

// planRemoval computes a dependency-safe removal order from a candidate set and
// its dependency edges: a job is removable only if every job that depends on it
// (afterok) is also a candidate and itself removable — so a parent is never
// deleted while a kept/active dependent still references it. Returns the
// post-order list (dependents before parents) and the count of candidates left
// blocked. Pure in-memory graph work; the same algorithm the old client-side
// planner ran, now fed by two bulk queries instead of N REST calls.
func planRemoval(candidateIDs []string, edges []storage.CleanupDepEdge) (order []string, blocked int) {
	cand := make(map[string]bool, len(candidateIDs))
	for _, id := range candidateIDs {
		cand[id] = true
	}
	dependents := make(map[string][]string)
	for _, e := range edges {
		dependents[e.AfterokID] = append(dependents[e.AfterokID], e.JobID)
	}

	const (
		stUnknown = iota
		stVisiting
		stRemovable
		stBlocked
	)
	state := make(map[string]int, len(candidateIDs))
	var canRemove func(id string) bool
	canRemove = func(id string) bool {
		if st, ok := state[id]; ok {
			if st == stVisiting { // cycle guard (job_deps is a DAG, but be safe)
				state[id] = stBlocked
				return false
			}
			return st == stRemovable
		}
		if !cand[id] {
			state[id] = stBlocked
			return false
		}
		state[id] = stVisiting
		for _, dep := range dependents[id] {
			if !canRemove(dep) {
				state[id] = stBlocked
				return false
			}
		}
		state[id] = stRemovable
		return true
	}

	added := make(map[string]bool, len(candidateIDs))
	var add func(id string)
	add = func(id string) {
		if added[id] || !canRemove(id) {
			return
		}
		for _, dep := range dependents[id] {
			add(dep)
		}
		if !added[id] {
			order = append(order, id)
			added[id] = true
		}
	}
	for _, id := range candidateIDs {
		add(id)
	}
	for _, id := range candidateIDs {
		if !canRemove(id) {
			blocked++
		}
	}
	return order, blocked
}

// Backup snapshots the database to destPath and returns the absolute path
// written. The path resolves against the SERVER's filesystem (the snapshot is
// taken by the server's own connection). When destPath is empty a default is
// chosen under the server's $BATCHQ_HOME/backups/ with a timestamped name.
// The parent directory is created; an already-existing destination is rejected
// (VACUUM INTO will not overwrite, and a clear error beats SQLite's raw one).
func (s *Service) Backup(ctx context.Context, destPath string) (string, error) {
	var path string
	if strings.TrimSpace(destPath) == "" {
		stamp := time.Now().Format("20060102-150405")
		path = filepath.Join(support.GetBatchqHome(), "backups", "batchq-"+stamp+".db")
	} else {
		abs, err := support.ExpandPathAbs(destPath)
		if err != nil {
			return "", fmt.Errorf("%w: bad backup path: %v", ErrBadRequest, err)
		}
		path = abs
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return "", fmt.Errorf("backup: create destination dir: %w", err)
	}
	if _, err := os.Stat(path); err == nil {
		return "", fmt.Errorf("%w: backup destination already exists: %s", ErrBadRequest, path)
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("backup: stat destination: %w", err)
	}
	if err := s.store.Backup(ctx, path); err != nil {
		return "", err
	}
	return path, nil
}

// --- Runner endpoints --------------------------------------------------

// ClaimNextJob is the atomic claim primitive exposed to runners. host is the
// hostname the runner advertised; when non-empty it is recorded as a "host"
// running detail on the claimed job so the queue/detail views can show which
// machine is running it.
func (s *Service) ClaimNextJob(ctx context.Context, runnerID, kind, host string, limits storage.Limits) (storage.ClaimResult, error) {
	if runnerID == "" {
		return storage.ClaimResult{}, fmt.Errorf("%w: runner_id required", ErrBadRequest)
	}
	if kind == "" {
		kind = "simple"
	}
	// Best-effort resolve before claiming so newly-eligible waiters become
	// candidates. Errors here are non-fatal — we still try the claim.
	_, _ = s.ResolveDependencies(ctx)
	res, err := s.store.ClaimNextJob(ctx, runnerID, kind, limits)
	if err != nil {
		return res, err
	}
	if res.Job != nil {
		s.recordHost(ctx, host, res.Job)
	}
	return res, nil
}

// recordHost persists the runner's advertised host as a "host" running detail
// on a freshly-claimed job and reflects it into the in-memory job so the claim
// response carries it. A no-op for an empty host; a failed write is logged but
// not fatal (the job is already claimed and running).
func (s *Service) recordHost(ctx context.Context, host string, job *jobs.JobDef) {
	if host == "" || job == nil {
		return
	}
	if err := s.store.UpdateRunningDetails(ctx, job.JobId, map[string]string{"host": host}); err != nil {
		log.Printf("service: record host for job %s: %v", job.JobId, err)
		return
	}
	job.RunningDetails = append(job.RunningDetails, jobs.JobRunningDetail{Key: "host", Value: host})
}

// ClaimNextArrayBatch claims the next eligible plain job or array batch for a
// batch-capable runner. See storage.ClaimNextArrayBatch.
func (s *Service) ClaimNextArrayBatch(ctx context.Context, runnerID, kind, host string, limits storage.Limits, maxTasks, minTasks int, fullArray bool) (storage.ArrayClaimResult, error) {
	if runnerID == "" {
		return storage.ArrayClaimResult{}, fmt.Errorf("%w: runner_id required", ErrBadRequest)
	}
	if kind == "" {
		kind = "slurm"
	}
	_, _ = s.ResolveDependencies(ctx)
	res, err := s.store.ClaimNextArrayBatch(ctx, runnerID, kind, limits, maxTasks, minTasks, fullArray)
	if err != nil {
		return res, err
	}
	if host != "" {
		// Plain-job batch: the single Job carries the host like ClaimNextJob.
		if res.Job != nil {
			s.recordHost(ctx, host, res.Job)
		}
		// Array batch: record host on every claimed task.
		for _, t := range res.Tasks {
			if derr := s.store.UpdateRunningDetails(ctx, t.JobID, map[string]string{"host": host}); derr != nil {
				log.Printf("service: record host for task %s: %v", t.JobID, derr)
			}
		}
	}
	return res, nil
}

func (s *Service) MarkJobProxied(ctx context.Context, runnerID, jobID string, runningDetails map[string]string) error {
	if err := s.store.MarkJobProxied(ctx, jobID, runnerID, runningDetails); err != nil {
		return err
	}
	// Handing a job to SLURM (-> PROXYQUEUED) can satisfy a downstream afterok
	// dependency, since ResolveDependencies treats PROXYQUEUED as met. Re-evaluate
	// waiting jobs — mirrors EndProxiedJob/EndJob. Without this, a gather depending
	// on an array stays WAITING until the next claim or a task completes, which the
	// runner may never reach if the array saturates its live-queue budget and
	// breaks out of the submit loop.
	_, _ = s.ResolveDependencies(ctx)
	return nil
}

func (s *Service) UpdateRunningDetails(ctx context.Context, jobID string, details map[string]string) error {
	if len(details) == 0 {
		return nil
	}
	return s.store.UpdateRunningDetails(ctx, jobID, details)
}

func (s *Service) EndJob(ctx context.Context, runnerID, jobID string, returnCode int, notes string) error {
	if err := s.store.EndJob(ctx, jobID, runnerID, returnCode, notes); err != nil {
		return err
	}
	// Success unblocks dependents; failure already cascades cancels.
	if returnCode == 0 {
		_, _ = s.ResolveDependencies(ctx)
	}
	return nil
}

func (s *Service) EndProxiedJob(ctx context.Context, jobID string, status jobs.StatusCode, startTime, endTime time.Time, returnCode int, notes string) error {
	if err := s.store.EndProxiedJob(ctx, jobID, status, startTime, endTime, returnCode, notes); err != nil {
		return err
	}
	if status == jobs.SUCCESS {
		_, _ = s.ResolveDependencies(ctx)
	}
	return nil
}

// --- Dependency resolution --------------------------------------------

// ResolveDependencies promotes waiting jobs whose dependencies are met and
// cancels those whose parents failed. Calls are serialized; a concurrent
// caller waits.
func (s *Service) ResolveDependencies(ctx context.Context) (storage.ResolveResult, error) {
	s.resolveMu.Lock()
	defer s.resolveMu.Unlock()
	return s.store.ResolveDependencies(ctx)
}

// Helpers ---------------------------------------------------------------

// ValidateJobID rejects obviously-malformed IDs at the service boundary
// before they reach storage.
func ValidateJobID(id string) error {
	if id == "" || strings.ContainsAny(id, " \t\n/") {
		return fmt.Errorf("%w: invalid job_id", ErrBadRequest)
	}
	return nil
}
