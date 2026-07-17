package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/compgenlab/batchq/api"
	"github.com/compgenlab/batchq/jobs"
	"github.com/compgenlab/batchq/service"
	"github.com/compgenlab/batchq/storage"
)

// --- helpers -----------------------------------------------------------

const maxRequestBody = 4 * 1024 * 1024 // 4 MiB — submit scripts can be sizable

func (s *Server) decode(r *http.Request, dst any) error {
	if r.Body == nil {
		return nil
	}
	limited := http.MaxBytesReader(nil, r.Body, maxRequestBody)
	defer limited.Close()
	dec := json.NewDecoder(limited)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return err
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if body != nil {
		_ = json.NewEncoder(w).Encode(body)
	}
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, api.ErrorResponse{Error: err.Error()})
}

// httpStatus maps a service-layer error to an HTTP status code.
func httpStatus(err error) int {
	switch {
	case err == nil:
		return http.StatusOK
	case errors.Is(err, service.ErrJobNotFound):
		return http.StatusNotFound
	case errors.Is(err, service.ErrBadRequest):
		return http.StatusBadRequest
	case errors.Is(err, service.ErrInvalidState):
		return http.StatusConflict
	case errors.Is(err, service.ErrForbidden):
		return http.StatusForbidden
	default:
		return http.StatusInternalServerError
	}
}

// pathID extracts a {param} from r.PathValue, returning a clear 400 if
// missing.
func pathID(r *http.Request, name string) (string, error) {
	v := r.PathValue(name)
	if v == "" {
		return "", errors.New("missing path parameter " + name)
	}
	return v, nil
}

// --- handlers ----------------------------------------------------------

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, api.HealthResponse{
		Status:     "ok",
		InstanceID: s.instanceID,
		PID:        os.Getpid(),
	})
}

func (s *Server) handleSubmitJob(w http.ResponseWriter, r *http.Request) {
	var req api.SubmitJobRequest
	if err := s.decode(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	dto, err := s.svc.SubmitJob(r.Context(), &req)
	if err != nil {
		writeError(w, httpStatus(err), err)
		return
	}
	writeJSON(w, http.StatusCreated, api.SubmitJobResponse{Job: dto})
}

func (s *Server) handleSubmitArray(w http.ResponseWriter, r *http.Request) {
	var req api.SubmitArrayRequest
	if err := s.decode(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	resp, err := s.svc.SubmitArray(r.Context(), &req)
	if err != nil {
		writeError(w, httpStatus(err), err)
		return
	}
	writeJSON(w, http.StatusCreated, resp)
}

func (s *Server) handleListJobs(w http.ResponseWriter, r *http.Request) {
	opts := service.ListJobsOptions{
		ShowAll:         r.URL.Query().Get("all") == "true",
		SortByStatus:    r.URL.Query().Get("sort_by_status") == "true",
		Query:           r.URL.Query().Get("q"),
		RunID:           r.URL.Query().Get("run_id"),
		ArrayID:         r.URL.Query().Get("array_id"),
		Output:          r.URL.Query().Get("output"),
		Input:           r.URL.Query().Get("input"),
		IncludeArchives: r.URL.Query().Get(api.QueryIncludeArchives) == "true",
	}
	var terr error
	if opts.Since, terr = queryTime(r, "since"); terr != nil {
		writeError(w, http.StatusBadRequest, terr)
		return
	}
	if opts.Before, terr = queryTime(r, "before"); terr != nil {
		writeError(w, http.StatusBadRequest, terr)
		return
	}
	if raw := r.URL.Query()["status"]; len(raw) > 0 {
		// status may appear once with comma-separated values or multiple times.
		var names []string
		for _, v := range raw {
			for _, p := range strings.Split(v, ",") {
				p = strings.TrimSpace(p)
				if p != "" {
					names = append(names, p)
				}
			}
		}
		statuses, err := api.ParseStatusList(names)
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		opts.Statuses = statuses
	}
	dtos, err := s.svc.ListJobs(r.Context(), opts)
	if err != nil {
		writeError(w, httpStatus(err), err)
		return
	}
	writeJSON(w, http.StatusOK, api.ListJobsResponse{Jobs: dtos})
}

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	includeArchives := r.URL.Query().Get(api.QueryIncludeArchives) == "true"
	dto, err := s.svc.GetJob(r.Context(), id, includeArchives)
	if err != nil {
		writeError(w, httpStatus(err), err)
		return
	}
	writeJSON(w, http.StatusOK, api.JobResponse{Job: dto})
}

func (s *Server) handleCancelJob(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	var req api.CancelJobRequest
	if err := s.decode(r, &req); err != nil {
		// DELETE bodies are optional — only reject if it was malformed JSON.
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := s.svc.CancelJob(r.Context(), id, req.Reason); err != nil {
		writeError(w, httpStatus(err), err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleJobDependents(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	deps, err := s.svc.GetJobDependents(r.Context(), id)
	if err != nil {
		writeError(w, httpStatus(err), err)
		return
	}
	writeJSON(w, http.StatusOK, api.JobDependentsResponse{JobIDs: deps})
}

func (s *Server) handleHoldJob(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := s.svc.HoldJob(r.Context(), id); err != nil {
		writeError(w, httpStatus(err), err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleReleaseJob(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := s.svc.ReleaseJob(r.Context(), id); err != nil {
		writeError(w, httpStatus(err), err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleCancelArray(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "array_id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	var req api.CancelJobRequest
	if err := s.decode(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	n, err := s.svc.CancelArray(r.Context(), id, req.Reason)
	if err != nil {
		writeError(w, httpStatus(err), err)
		return
	}
	writeJSON(w, http.StatusOK, api.ArrayActionResponse{Count: n})
}

func (s *Server) handleHoldArray(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "array_id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	n, err := s.svc.HoldArray(r.Context(), id)
	if err != nil {
		writeError(w, httpStatus(err), err)
		return
	}
	writeJSON(w, http.StatusOK, api.ArrayActionResponse{Count: n})
}

func (s *Server) handleReleaseArray(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "array_id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	n, err := s.svc.ReleaseArray(r.Context(), id)
	if err != nil {
		writeError(w, httpStatus(err), err)
		return
	}
	writeJSON(w, http.StatusOK, api.ArrayActionResponse{Count: n})
}

func (s *Server) handleArrayPriority(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "array_id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	var req api.PriorityRequest
	if err := s.decode(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	n, err := s.svc.AdjustArrayPriority(r.Context(), id, req.Delta)
	if err != nil {
		writeError(w, httpStatus(err), err)
		return
	}
	writeJSON(w, http.StatusOK, api.ArrayActionResponse{Count: n})
}

func (s *Server) handlePriority(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	var req api.PriorityRequest
	if err := s.decode(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := s.svc.AdjustJobPriority(r.Context(), id, req.Delta); err != nil {
		writeError(w, httpStatus(err), err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleCleanupJob(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := s.svc.CleanupJob(r.Context(), id); err != nil {
		writeError(w, httpStatus(err), err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleQueue(w http.ResponseWriter, r *http.Request) {
	showAll := r.URL.Query().Get("all") == "true"
	sortByStatus := r.URL.Query().Get("sort_by_status") == "true"
	since, terr := queryTime(r, "since")
	if terr != nil {
		writeError(w, http.StatusBadRequest, terr)
		return
	}
	before, terr := queryTime(r, "before")
	if terr != nil {
		writeError(w, http.StatusBadRequest, terr)
		return
	}
	dtos, err := s.svc.GetQueueJobs(r.Context(), showAll, sortByStatus, since, before)
	if err != nil {
		writeError(w, httpStatus(err), err)
		return
	}
	writeJSON(w, http.StatusOK, api.ListJobsResponse{Jobs: dtos})
}

// queryTime parses an RFC3339 query parameter, returning the zero time when
// the parameter is absent.
func queryTime(r *http.Request, key string) (time.Time, error) {
	v := r.URL.Query().Get(key)
	if v == "" {
		return time.Time{}, nil
	}
	t, err := time.Parse(time.RFC3339, v)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid %s time %q: %w", key, v, err)
	}
	return t, nil
}

func (s *Server) handleQueueCounts(w http.ResponseWriter, r *http.Request) {
	showAll := r.URL.Query().Get("all") == "true"
	counts, err := s.svc.GetJobStatusCounts(r.Context(), showAll)
	if err != nil {
		writeError(w, httpStatus(err), err)
		return
	}
	writeJSON(w, http.StatusOK, api.StatusCountsResponse{Counts: counts})
}

// handleBackup snapshots the DB via the service (VACUUM INTO on the server's
// own connection) and returns the absolute server-side path written. Unlike
// handleShutdown it runs synchronously so the response carries the result.
func (s *Server) handleBackup(w http.ResponseWriter, r *http.Request) {
	var req api.BackupRequest
	if err := s.decode(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	path, err := s.svc.Backup(r.Context(), req.Destination)
	if err != nil {
		writeError(w, httpStatus(err), err)
		return
	}
	writeJSON(w, http.StatusOK, api.BackupResponse{Path: path})
}

// handleVacuum runs VACUUM on the live DB via the service. Synchronous — may be
// slow on a large DB, so clients must allow a long timeout.
func (s *Server) handleVacuum(w http.ResponseWriter, r *http.Request) {
	if err := s.svc.Vacuum(r.Context()); err != nil {
		writeError(w, httpStatus(err), err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleCleanup runs a server-side bulk cleanup (select + dependency-safe
// delete/archive + optional vacuum) and STREAMS progress back as newline-
// delimited JSON (api.CleanupEvent), so a long purge on a large DB reports live
// instead of blocking silently. Request validation happens before any stream
// bytes are written; once streaming starts, a failure is reported as an "error"
// event (the HTTP status is already 200). The stream always ends with a "done"
// or "error" event.
func (s *Server) handleCleanup(w http.ResponseWriter, r *http.Request) {
	var req api.CleanupRequest
	if err := s.decode(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	statuses, err := api.ParseStatusList(req.Statuses)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	w.Header().Set("Content-Type", "application/x-ndjson")
	w.WriteHeader(http.StatusOK)
	enc := json.NewEncoder(w)
	flusher, _ := w.(http.Flusher)
	emit := func(ev api.CleanupEvent) {
		_ = enc.Encode(ev) // Encode appends a newline → one JSON object per line
		if flusher != nil {
			flusher.Flush()
		}
	}

	res, err := s.svc.CleanupBulk(r.Context(), service.CleanupBulkOptions{
		Statuses:    statuses,
		OlderThan:   time.Duration(req.OlderThanSecs) * time.Second,
		Archive:     req.Archive,
		ArchiveName: req.ArchiveName,
		Vacuum:      req.Vacuum,
	}, emit)
	if err != nil {
		emit(api.CleanupEvent{Phase: api.CleanupPhaseError, Error: err.Error()})
		return
	}
	emit(api.CleanupEvent{
		Phase:       api.CleanupPhaseDone,
		Removed:     res.Removed,
		Blocked:     res.Blocked,
		ArchivePath: res.ArchivePath,
		Vacuumed:    res.Vacuumed,
	})
}

// handleArchiveJobs moves a set of terminal jobs into a new read-only archive DB
// on the server's filesystem and returns its path plus the count archived.
func (s *Server) handleArchiveJobs(w http.ResponseWriter, r *http.Request) {
	var req api.ArchiveJobsRequest
	if err := s.decode(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	path, count, err := s.svc.ArchiveJobs(r.Context(), req.IDs, req.ArchiveName)
	if err != nil {
		writeError(w, httpStatus(err), err)
		return
	}
	writeJSON(w, http.StatusOK, api.ArchiveJobsResponse{Path: path, Count: count})
}

// --- runner handlers ---------------------------------------------------

func (s *Server) handleClaim(w http.ResponseWriter, r *http.Request) {
	runnerID, err := pathID(r, "runner_id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	var req api.ClaimJobRequest
	if err := s.decode(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	result, err := s.svc.ClaimNextJob(r.Context(), runnerID, req.Kind, req.Host, storage.Limits{
		MaxProcs:       req.MaxProcs,
		MaxMemoryMB:    req.MaxMemoryMB,
		MaxWalltimeSec: req.MaxWalltimeSec,
		Resources:      req.Resources,
	})
	if err != nil {
		writeError(w, httpStatus(err), err)
		return
	}
	if result.Job == nil {
		writeJSON(w, http.StatusOK, api.ClaimJobResponse{
			MoreEligible: result.MoreEligible,
			Blocked:      result.Blocked,
		})
		return
	}
	writeJSON(w, http.StatusOK, api.ClaimJobResponse{
		Job:          api.JobFromDef(result.Job),
		MoreEligible: result.MoreEligible,
		Blocked:      result.Blocked,
	})
}

func (s *Server) handleClaimArray(w http.ResponseWriter, r *http.Request) {
	runnerID, err := pathID(r, "runner_id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	var req api.ClaimArrayRequest
	if err := s.decode(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	result, err := s.svc.ClaimNextArrayBatch(r.Context(), runnerID, req.Kind, req.Host, storage.Limits{
		MaxProcs:       req.MaxProcs,
		MaxMemoryMB:    req.MaxMemoryMB,
		MaxWalltimeSec: req.MaxWalltimeSec,
		Resources:      req.Resources,
	}, req.MaxTasks, req.MinTasks, req.FullArray)
	if err != nil {
		writeError(w, httpStatus(err), err)
		return
	}
	resp := api.ClaimArrayResponse{
		ArrayID:      result.ArrayID,
		Throttle:     result.Throttle,
		MoreEligible: result.MoreEligible,
		Blocked:      result.Blocked,
		Deferred:     result.Deferred,
	}
	if result.Job != nil {
		resp.Job = api.JobFromDef(result.Job)
	}
	for _, t := range result.Tasks {
		resp.Tasks = append(resp.Tasks, api.ArrayTaskDTO{JobID: t.JobID, Index: t.Index})
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleMarkProxied(w http.ResponseWriter, r *http.Request) {
	runnerID, err := pathID(r, "runner_id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	jobID, err := pathID(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	var req api.ProxyJobRequest
	if err := s.decode(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := s.svc.MarkJobProxied(r.Context(), runnerID, jobID, req.RunningDetails); err != nil {
		writeError(w, httpStatus(err), err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleUpdateRunning(w http.ResponseWriter, r *http.Request) {
	jobID, err := pathID(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	var req api.RunningDetailsRequest
	if err := s.decode(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := s.svc.UpdateRunningDetails(r.Context(), jobID, req.Details); err != nil {
		writeError(w, httpStatus(err), err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleEndJob(w http.ResponseWriter, r *http.Request) {
	runnerID, err := pathID(r, "runner_id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	jobID, err := pathID(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	var req api.EndJobRequest
	if err := s.decode(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := s.svc.EndJob(r.Context(), runnerID, jobID, req.ReturnCode, req.Notes); err != nil {
		writeError(w, httpStatus(err), err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleEndProxied(w http.ResponseWriter, r *http.Request) {
	jobID, err := pathID(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	var req api.EndProxyRequest
	if err := s.decode(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	status, err := api.ParseStatus(req.Status)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	switch status {
	case jobs.SUCCESS, jobs.FAILED, jobs.CANCELED:
	default:
		writeError(w, http.StatusBadRequest, errors.New("status must be terminal (SUCCESS/FAILED/CANCELED)"))
		return
	}
	if err := s.svc.EndProxiedJob(r.Context(), jobID, status,
		derefTime(req.StartTime), derefTime(req.EndTime), req.ReturnCode, req.Notes); err != nil {
		writeError(w, httpStatus(err), err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// derefTime returns t or the zero time if t is nil.
func derefTime(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return *t
}

// handleShutdown answers OK and then asks the http.Server to drain in a
// goroutine. Returning to the caller flushes the response; once the
// handler unwinds, Shutdown's "wait for in-flight" closes the connection
// and unbinds the listener. The whole process exits when Serve unblocks.
//
// This endpoint is intended for the local unix socket — file permissions
// are the auth boundary. Remote deployments should gate /admin/* at the
// reverse proxy.
func (s *Server) handleShutdown(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "shutting down"})
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		// Ordered teardown (DB closed before the socket is unlinked). This
		// request is itself in flight, so shutdown takes the "busy" path:
		// drain (this handler has already returned its response) then close.
		_ = s.shutdown(ctx, "admin", false)
	}()
}
