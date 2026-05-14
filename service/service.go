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
func (s *Service) SubmitJob(ctx context.Context, req *api.SubmitJobRequest) (*api.JobDTO, error) {
	if req == nil {
		return nil, ErrBadRequest
	}
	if _, ok := req.Details["script"]; !ok {
		return nil, fmt.Errorf("%w: missing details.script", ErrBadRequest)
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

func (s *Service) CancelJob(ctx context.Context, jobID, reason string) error {
	if reason == "" {
		reason = "user request"
	}
	return s.store.CancelJob(ctx, jobID, reason)
}

func (s *Service) HoldJob(ctx context.Context, jobID string) error {
	return s.store.HoldJob(ctx, jobID)
}

func (s *Service) ReleaseJob(ctx context.Context, jobID string) error {
	if err := s.store.ReleaseJob(ctx, jobID); err != nil {
		return err
	}
	// A just-released job may now be eligible to QUEUE; trigger a pass.
	_, _ = s.ResolveDependencies(ctx)
	return nil
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

func (s *Service) EndJob(ctx context.Context, runnerID, jobID string, returnCode int) error {
	if err := s.store.EndJob(ctx, jobID, runnerID, returnCode); err != nil {
		return err
	}
	// Success unblocks dependents; failure already cascades cancels.
	if returnCode == 0 {
		_, _ = s.ResolveDependencies(ctx)
	}
	return nil
}

func (s *Service) EndProxiedJob(ctx context.Context, jobID string, status jobs.StatusCode, startTime, endTime time.Time, returnCode int) error {
	if err := s.store.EndProxiedJob(ctx, jobID, status, startTime, endTime, returnCode); err != nil {
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
