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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mbreese/batchq/api"
	"github.com/mbreese/batchq/jobs"
	"github.com/mbreese/batchq/storage"
	"github.com/mbreese/batchq/support"
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

	// resolveMu serializes dep-resolution work so a burst of state
	// transitions only triggers one ResolveDependencies pass.
	resolveMu sync.Mutex
}

// New returns a Service backed by the given Storage.
func New(s storage.Storage) *Service {
	return &Service{store: s}
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

// expandSingleDeps resolves dependency specs for a single (non-array) dependent.
// afterok on an array fans out to all its members; aftercorr is invalid.
func (s *Service) expandSingleDeps(ctx context.Context, specs []depSpec) ([]string, error) {
	var afterok []string
	for _, sp := range specs {
		if sp.kind == "aftercorr" {
			return nil, fmt.Errorf("%w: aftercorr requires an array dependent", ErrBadRequest)
		}
		members, err := s.loadArrayMembers(ctx, sp.target)
		if err != nil {
			return nil, err
		}
		if members == nil {
			afterok = append(afterok, sp.target)
			continue
		}
		for _, m := range members {
			afterok = append(afterok, m.id)
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

func (s *Service) GetJob(ctx context.Context, jobID string) (*api.JobDTO, error) {
	job, err := s.store.GetJob(ctx, jobID)
	if err != nil {
		return nil, err
	}
	return api.JobFromDef(job), nil
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
}

func (s *Service) ListJobs(ctx context.Context, opts ListJobsOptions) ([]*api.JobDTO, error) {
	var (
		out []*jobs.JobDef
		err error
	)
	switch {
	case opts.Query != "":
		out, err = s.store.SearchJobs(ctx, opts.Query, opts.Statuses)
	case len(opts.Statuses) > 0:
		out, err = s.store.ListJobsByStatus(ctx, opts.Statuses, opts.SortByStatus)
	default:
		out, err = s.store.ListJobs(ctx, opts.ShowAll, opts.SortByStatus)
	}
	if err != nil {
		return nil, err
	}

	if opts.RunID != "" || opts.ArrayID != "" || opts.Output != "" || opts.Input != "" {
		var allow map[string]struct{}
		if opts.RunID != "" {
			ids, err := s.store.FindJobsByDetail(ctx, "run_id", opts.RunID)
			if err != nil {
				return nil, err
			}
			allow = intersect(allow, ids)
		}
		if opts.ArrayID != "" {
			ids, err := s.store.FindJobsByDetail(ctx, "array_id", opts.ArrayID)
			if err != nil {
				return nil, err
			}
			allow = intersect(allow, ids)
		}
		if opts.Output != "" {
			ids, err := s.store.FindJobsByOutputPath(ctx, opts.Output)
			if err != nil {
				return nil, err
			}
			allow = intersect(allow, ids)
		}
		if opts.Input != "" {
			ids, err := s.store.FindJobsByInputPath(ctx, opts.Input)
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
	}

	return toDTOs(out), nil
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

func (s *Service) GetQueueJobs(ctx context.Context, showAll, sortByStatus bool) ([]*api.JobDTO, error) {
	out, err := s.store.GetQueueJobs(ctx, showAll, sortByStatus)
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
	return s.store.MarkJobProxied(ctx, jobID, runnerID, runningDetails)
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
