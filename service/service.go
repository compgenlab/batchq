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

	job := &jobs.JobDef{
		JobId:       support.NewUUID(),
		Name:        req.Name,
		Notes:       req.Notes,
		Priority:    req.Priority,
		AfterOk:     req.AfterOk,
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

	// RunID, Produces, and Consumes are optional intersect-filters
	// applied after the base listing. Empty values are ignored.
	RunID    string
	Produces string
	Consumes string
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

	if opts.RunID != "" || opts.Produces != "" || opts.Consumes != "" {
		var allow map[string]struct{}
		if opts.RunID != "" {
			ids, err := s.store.FindJobsByDetail(ctx, "run_id", opts.RunID)
			if err != nil {
				return nil, err
			}
			allow = intersect(allow, ids)
		}
		if opts.Produces != "" {
			ids, err := s.store.FindJobsByOutputPath(ctx, opts.Produces)
			if err != nil {
				return nil, err
			}
			allow = intersect(allow, ids)
		}
		if opts.Consumes != "" {
			ids, err := s.store.FindJobsByInputPath(ctx, opts.Consumes)
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

// --- Runner endpoints --------------------------------------------------

// ClaimNextJob is the atomic claim primitive exposed to runners.
func (s *Service) ClaimNextJob(ctx context.Context, runnerID, kind string, limits storage.Limits) (storage.ClaimResult, error) {
	if runnerID == "" {
		return storage.ClaimResult{}, fmt.Errorf("%w: runner_id required", ErrBadRequest)
	}
	if kind == "" {
		kind = "simple"
	}
	// Best-effort resolve before claiming so newly-eligible waiters become
	// candidates. Errors here are non-fatal — we still try the claim.
	_, _ = s.ResolveDependencies(ctx)
	return s.store.ClaimNextJob(ctx, runnerID, kind, limits)
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
