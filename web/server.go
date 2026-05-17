package web

import (
	"bytes"
	"context"
	"embed"
	"errors"
	"fmt"
	"html/template"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/mbreese/batchq/api"
	"github.com/mbreese/batchq/client"
	"github.com/mbreese/batchq/jobs"
	"github.com/mbreese/batchq/support"
)

//go:embed templates/*.html
var webTemplatesFS embed.FS

const webRequestTimeout = 2 * time.Minute

type Options struct {
	Config     *support.Config
	Client     *client.Client
	SocketPath string
	ListenAddr string
	Force      bool
	Verbose    bool
}

type webServer struct {
	client    *client.Client
	templates *template.Template
	verbose   bool
}

type detailRow struct {
	Key   string
	Value string
	IsPre bool
}

type jobTreeNode struct {
	Job      *jobs.JobDef
	Children []*jobTreeNode
}

type queuePage struct {
	Title           string
	ContentTemplate string
	Jobs            []*jobs.JobDef
	ShowAll         bool
	Query           string
	StatusOptions   []string
	SelectedStatus  map[string]bool
}

type jobPage struct {
	Title           string
	ContentTemplate string
	Job             *jobs.JobDef
	Parents         []*jobs.JobDef
	Children        []*jobs.JobDef
	DetailRows      []detailRow
	RunningRows     []detailRow
	Script          string
	SlurmScript     string
	ParentTree      *jobTreeNode
	ChildTree       *jobTreeNode
	Query           string
}

func StartServer(opts Options) error {
	log.SetOutput(os.Stderr)
	if opts.Client == nil {
		return errors.New("web: client required")
	}
	socketPath, err := resolveSocketPath(opts)
	if err != nil {
		return err
	}
	listener, kind, address, cleanup, err := createListener(socketPath, resolveListenAddress(opts), opts.Force)
	if err != nil {
		return err
	}
	defer cleanup()

	templates, err := loadWebTemplates()
	if err != nil {
		return err
	}

	server := &webServer{client: opts.Client, templates: templates, verbose: opts.Verbose}
	mux := http.NewServeMux()
	// Go 1.22+ path patterns — {id} captured via r.PathValue.
	mux.HandleFunc("GET /jobs/{id}/logs/{stream}", server.handleJobLogs)
	mux.HandleFunc("GET /jobs/{id}", server.handleJob)
	mux.HandleFunc("GET /jobs", server.handleQueue)
	mux.HandleFunc("GET /", server.handleQueue)

	httpServer := &http.Server{
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	shutdownCh := make(chan os.Signal, 1)
	signal.Notify(shutdownCh, os.Interrupt, syscall.SIGTERM)

	// Always print the listening address — operators need to know
	// what URL to hit. --verbose stays meaningful for per-request
	// logging via s.logf.
	var displayURL string
	switch kind {
	case "tcp":
		displayURL = "http://" + address
	case "unix":
		displayURL = "unix://" + address
	default:
		displayURL = kind + ":" + address
	}
	fmt.Fprintf(os.Stderr, "batchq web listening on %s\n", displayURL)

	go func() {
		if err := httpServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("web server error: %v", err)
		}
	}()

	// Keepalive: ping the batchq server every keepaliveInterval so its
	// idle-timeout never fires while the web UI is up. The web process
	// being alive implies users may interact at any moment; we don't
	// want the batchq server to evict itself underneath us.
	keepaliveCtx, keepaliveCancel := context.WithCancel(context.Background())
	defer keepaliveCancel()
	go server.runKeepalive(keepaliveCtx)

	<-shutdownCh
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return httpServer.Shutdown(ctx)
}

func resolveSocketPath(opts Options) (string, error) {
	if opts.SocketPath != "" {
		return normalizeSocketPath(opts.SocketPath)
	}
	if opts.Config != nil && opts.Config.Web.Socket != "" {
		return normalizeSocketPath(opts.Config.Web.Socket)
	}
	return normalizeSocketPath(support.NewDefaults().WebSocket)
}

func resolveListenAddress(opts Options) string {
	if opts.ListenAddr != "" {
		return opts.ListenAddr
	}
	if opts.Config == nil {
		return ""
	}
	return opts.Config.Web.Listen
}

func normalizeSocketPath(path string) (string, error) {
	if path == "" || path == "-" {
		return "", nil
	}
	return support.ExpandPathAbs(path)
}

func removeSocketIfExists(socketPath string, force bool) error {
	if socketPath == "" {
		return nil
	}
	if _, err := os.Stat(socketPath); err == nil {
		if !force {
			return errors.New("socket exists; remove it or pass --force")
		}
		return os.Remove(socketPath)
	} else if !os.IsNotExist(err) {
		return err
	}
	return nil
}

func createListener(socketPath string, listenAddr string, force bool) (net.Listener, string, string, func(), error) {
	tcpAddr := strings.TrimSpace(listenAddr)

	if socketPath != "" && tcpAddr != "" {
		return nil, "", "", func() {}, errors.New("configure only one of web_socket or web_listen (or --socket/--listen)")
	}

	if socketPath != "" {
		if err := removeSocketIfExists(socketPath, force); err != nil {
			return nil, "", "", func() {}, err
		}
		unixListener, err := net.Listen("unix", socketPath)
		if err != nil {
			return nil, "", "", func() {}, err
		}
		cleanup := func() {
			unixListener.Close()
			os.Remove(socketPath)
		}
		return unixListener, "unix", socketPath, cleanup, nil
	}

	if tcpAddr != "" {
		tcpListener, err := net.Listen("tcp", tcpAddr)
		if err != nil {
			return nil, "", "", func() {}, err
		}
		cleanup := func() {
			tcpListener.Close()
		}
		return tcpListener, "tcp", tcpAddr, cleanup, nil
	}

	return nil, "", "", func() {}, errors.New("no listener configured; set --socket or --listen (or config web_socket/web_listen)")
}

func loadWebTemplates() (*template.Template, error) {
	funcs := template.FuncMap{
		"formatTime": func(t time.Time) string {
			if t.IsZero() {
				return ""
			}
			return t.Format("2006-01-02 15:04:05")
		},
		"statusClass": func(status jobs.StatusCode) string {
			return strings.ToLower(status.String())
		},
		"lower": strings.ToLower,
		// Display helpers for detail values stored as raw strings.
		"memDisplay":      jobs.PrintMemoryString,
		"walltimeDisplay": jobs.WalltimeStringToString,
		"envCount":        countEnvEntries,
		// truthy reports whether a string is non-empty after trimming.
		"truthy": func(s string) bool { return strings.TrimSpace(s) != "" },
		// logPath returns the (substituted, wd-anchored) path of the
		// requested stdout/stderr stream for the given job.
		"logPath": resolveLogPath,
		// elapsed renders a duration between two times, e.g. "1h 32m 4s".
		"elapsed": func(start, end time.Time) string {
			if start.IsZero() || end.IsZero() {
				return ""
			}
			d := end.Sub(start).Round(time.Second)
			if d < 0 {
				return ""
			}
			return d.String()
		},
	}

	return template.New("base").Funcs(funcs).ParseFS(webTemplatesFS, "templates/*.html")
}

func (s *webServer) handleQueue(w http.ResponseWriter, r *http.Request) {
	s.logf("web request %s %s", r.Method, r.URL.Path)
	if r.URL.Path != "/" && r.URL.Path != "/jobs" {
		http.NotFound(w, r)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), webRequestTimeout)
	defer cancel()

	statuses, selected := parseStatusFilter(r)
	query := strings.TrimSpace(r.URL.Query().Get("q"))

	// With a query, the queue becomes a search view over all jobs
	// (completed included) — finding "that one job" is the use case.
	// Without a query, the standard queue path applies status filtering.
	var jobList []*jobs.JobDef
	var err error
	if query != "" {
		dtos, lerr := s.client.ListJobs(ctx, client.ListJobsOptions{
			ShowAll:  true,
			Query:    query,
			Statuses: statusStrings(statuses),
		})
		if lerr != nil {
			s.serveError(w, r, "search", lerr)
			return
		}
		jobList = dtosToJobDefs(dtos)
	} else {
		jobList, err = s.jobsForFilter(ctx, statuses)
		if err != nil {
			s.serveError(w, r, "queue lookup", err)
			return
		}
	}
	showAll := len(statuses) == 0
	title := "batchq queue"
	if query != "" {
		title = "batchq search · " + query
	}
	data := queuePage{
		Title:           title,
		ContentTemplate: "queue-content",
		Query:           query,
		Jobs:           jobList,
		ShowAll:        showAll,
		StatusOptions:  statusOptions(),
		SelectedStatus: selected,
	}

	s.render(w, r, data)
}

func (s *webServer) handleJob(w http.ResponseWriter, r *http.Request) {
	s.logf("web request %s %s", r.Method, r.URL.Path)
	jobId := r.PathValue("id")
	if jobId == "" {
		http.NotFound(w, r)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), webRequestTimeout)
	defer cancel()

	job, err := s.getJob(ctx, jobId)
	if err != nil {
		s.serveError(w, r, "job lookup", err)
		return
	}
	if job == nil {
		http.NotFound(w, r)
		return
	}

	parents, err := s.fetchJobs(ctx, job.AfterOk)
	if err != nil {
		s.serveError(w, r, "parents lookup", err)
		return
	}
	dependentIDs, err := s.client.GetJobDependents(ctx, job.JobId)
	if err != nil {
		s.serveError(w, r, "dependents lookup", err)
		return
	}
	children, err := s.fetchJobs(ctx, dependentIDs)
	if err != nil {
		s.serveError(w, r, "children lookup", err)
		return
	}

	detailRows, script := buildDetailRows(job.Details, "script")
	runningRows, slurmScript := buildDetailRowsRunning(job.RunningDetails, "slurm_script")
	var parentTree *jobTreeNode
	if len(parents) > 0 {
		parentTree, err = s.buildParentTree(ctx, job.JobId, map[string]bool{})
		if err != nil {
			s.serveError(w, r, "parent tree", err)
			return
		}
	}
	var childTree *jobTreeNode
	if len(children) > 0 {
		childTree, err = s.buildChildTree(ctx, job.JobId, map[string]bool{})
		if err != nil {
			s.serveError(w, r, "child tree", err)
			return
		}
	}

	data := jobPage{
		Title:           "batchq job " + job.JobId,
		ContentTemplate: "job-content",
		Job:             job,
		Parents:         parents,
		Children:        children,
		DetailRows:      detailRows,
		RunningRows:     runningRows,
		Script:          script,
		SlurmScript:     slurmScript,
		ParentTree:      parentTree,
		ChildTree:       childTree,
		Query:           "",
	}

	s.render(w, r, data)
}

func (s *webServer) render(w http.ResponseWriter, r *http.Request, data any) {
	var buf bytes.Buffer
	if err := s.templates.ExecuteTemplate(&buf, "base", data); err != nil {
		log.Printf("template error for %s: %v", r.URL.Path, err)
		http.Error(w, "Template error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(buf.Bytes())
}

func (s *webServer) serveError(w http.ResponseWriter, r *http.Request, what string, err error) {
	log.Printf("web %s: %s: %v", r.URL.Path, what, err)
	http.Error(w, fmt.Sprintf("batchq server error: %s", what), http.StatusBadGateway)
}

func (s *webServer) logf(format string, args ...any) {
	if s.verbose {
		log.Printf(format, args...)
	}
}

// getJob fetches a job and converts the DTO to a *jobs.JobDef for the
// templates. Returns (nil, nil) when the job is not found.
func (s *webServer) getJob(ctx context.Context, jobID string) (*jobs.JobDef, error) {
	dto, err := s.client.GetJob(ctx, jobID)
	if err != nil {
		if errors.Is(err, client.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return dtoToJobDef(dto), nil
}

func (s *webServer) fetchJobs(ctx context.Context, ids []string) ([]*jobs.JobDef, error) {
	var list []*jobs.JobDef
	for _, id := range ids {
		job, err := s.getJob(ctx, id)
		if err != nil {
			return nil, err
		}
		if job != nil {
			list = append(list, job)
		}
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].JobId < list[j].JobId
	})
	return list, nil
}

// dtoToJobDef wraps api.JobToDef and additionally parses the wire-format
// status string back into a StatusCode so templates can render it.
func dtoToJobDef(dto *api.JobDTO) *jobs.JobDef {
	if dto == nil {
		return nil
	}
	def := api.JobToDef(dto)
	def.Status = parseStatusCode(dto.Status)
	return def
}

func dtosToJobDefs(dtos []*api.JobDTO) []*jobs.JobDef {
	out := make([]*jobs.JobDef, 0, len(dtos))
	for _, dto := range dtos {
		if def := dtoToJobDef(dto); def != nil {
			out = append(out, def)
		}
	}
	return out
}

func buildDetailRows(details []jobs.JobDefDetail, scriptKey string) ([]detailRow, string) {
	var rows []detailRow
	script := ""
	for _, detail := range details {
		if detail.Key == scriptKey {
			script = detail.Value
			continue
		}
		row := detailRow{Key: detail.Key, Value: detail.Value}
		switch detail.Key {
		case "walltime":
			row.Value = jobs.WalltimeStringToString(detail.Value)
		case "mem":
			row.Value = jobs.PrintMemoryString(detail.Value)
		case "env":
			row.Value = fmt.Sprintf("%d variables captured", countEnvEntries(detail.Value))
		}
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].Key < rows[j].Key
	})
	return rows, script
}

func countEnvEntries(raw string) int {
	if strings.TrimSpace(raw) == "" {
		return 0
	}
	return len(strings.Split(raw, "\n-|-\n"))
}

func buildDetailRowsRunning(details []jobs.JobRunningDetail, scriptKey string) ([]detailRow, string) {
	var rows []detailRow
	script := ""
	for _, detail := range details {
		if detail.Key == scriptKey {
			script = detail.Value
			continue
		}
		row := detailRow{Key: detail.Key, Value: detail.Value}
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].Key < rows[j].Key
	})
	return rows, script
}

func (s *webServer) buildParentTree(ctx context.Context, jobId string, visited map[string]bool) (*jobTreeNode, error) {
	job, err := s.getJob(ctx, jobId)
	if err != nil {
		return nil, err
	}
	if job == nil {
		return nil, nil
	}
	node := &jobTreeNode{Job: job}
	if visited[jobId] {
		return node, nil
	}
	visited[jobId] = true
	defer delete(visited, jobId)
	for _, parentId := range job.AfterOk {
		parent, err := s.buildParentTree(ctx, parentId, visited)
		if err != nil {
			return nil, err
		}
		if parent != nil {
			node.Children = append(node.Children, parent)
		}
	}
	sort.Slice(node.Children, func(i, j int) bool {
		return node.Children[i].Job.JobId < node.Children[j].Job.JobId
	})
	return node, nil
}

func (s *webServer) buildChildTree(ctx context.Context, jobId string, visited map[string]bool) (*jobTreeNode, error) {
	job, err := s.getJob(ctx, jobId)
	if err != nil {
		return nil, err
	}
	if job == nil {
		return nil, nil
	}
	node := &jobTreeNode{Job: job}
	if visited[jobId] {
		return node, nil
	}
	visited[jobId] = true
	defer delete(visited, jobId)
	childIds, err := s.client.GetJobDependents(ctx, jobId)
	if err != nil {
		return nil, err
	}
	for _, childId := range childIds {
		child, err := s.buildChildTree(ctx, childId, visited)
		if err != nil {
			return nil, err
		}
		if child != nil {
			node.Children = append(node.Children, child)
		}
	}
	sort.Slice(node.Children, func(i, j int) bool {
		return node.Children[i].Job.JobId < node.Children[j].Job.JobId
	})
	return node, nil
}

func (s *webServer) jobsForFilter(ctx context.Context, statuses []jobs.StatusCode) ([]*jobs.JobDef, error) {
	var (
		dtos []*api.JobDTO
		err  error
	)
	switch {
	case len(statuses) == 0:
		dtos, err = s.client.GetQueueJobs(ctx, true, true)
	case isActiveStatusSet(statuses):
		dtos, err = s.client.GetQueueJobs(ctx, false, true)
	default:
		dtos, err = s.client.ListJobs(ctx, client.ListJobsOptions{
			Statuses:     statusStrings(statuses),
			SortByStatus: true,
		})
	}
	if err != nil {
		return nil, err
	}
	return dtosToJobDefs(dtos), nil
}

func parseStatusFilter(r *http.Request) ([]jobs.StatusCode, map[string]bool) {
	selected := make(map[string]bool)
	statuses := parseStatusList(r.URL.Query()["status"], selected)
	if len(statuses) == 0 {
		statuses = activeStatuses()
		for _, status := range statuses {
			selected[status.String()] = true
		}
	}
	return statuses, selected
}

func parseStatusList(raw []string, selected map[string]bool) []jobs.StatusCode {
	if selected == nil {
		selected = make(map[string]bool)
	}
	lookup := statusLookup()
	var statuses []jobs.StatusCode
	for _, val := range raw {
		key := strings.ToUpper(strings.TrimSpace(val))
		if key == "" {
			continue
		}
		code, ok := lookup[key]
		if !ok {
			continue
		}
		selected[key] = true
		statuses = append(statuses, code)
	}
	return statuses
}

func statusLookup() map[string]jobs.StatusCode {
	return map[string]jobs.StatusCode{
		jobs.UNKNOWN.String():     jobs.UNKNOWN,
		jobs.USERHOLD.String():    jobs.USERHOLD,
		jobs.WAITING.String():     jobs.WAITING,
		jobs.QUEUED.String():      jobs.QUEUED,
		jobs.PROXYQUEUED.String(): jobs.PROXYQUEUED,
		jobs.RUNNING.String():     jobs.RUNNING,
		jobs.SUCCESS.String():     jobs.SUCCESS,
		jobs.FAILED.String():      jobs.FAILED,
		jobs.CANCELED.String():    jobs.CANCELED,
	}
}

func parseStatusCode(name string) jobs.StatusCode {
	if code, ok := statusLookup()[strings.ToUpper(strings.TrimSpace(name))]; ok {
		return code
	}
	return jobs.UNKNOWN
}

func statusStrings(codes []jobs.StatusCode) []string {
	out := make([]string, 0, len(codes))
	for _, c := range codes {
		out = append(out, c.String())
	}
	return out
}

func statusOptions() []string {
	return []string{
		jobs.USERHOLD.String(),
		jobs.WAITING.String(),
		jobs.QUEUED.String(),
		jobs.PROXYQUEUED.String(),
		jobs.RUNNING.String(),
		jobs.SUCCESS.String(),
		jobs.FAILED.String(),
		jobs.CANCELED.String(),
	}
}

func activeStatuses() []jobs.StatusCode {
	return []jobs.StatusCode{
		jobs.WAITING,
		jobs.QUEUED,
		jobs.PROXYQUEUED,
		jobs.RUNNING,
	}
}

func isActiveStatusSet(statuses []jobs.StatusCode) bool {
	if len(statuses) != len(activeStatuses()) {
		return false
	}
	activeLookup := map[jobs.StatusCode]bool{}
	for _, status := range activeStatuses() {
		activeLookup[status] = true
	}
	for _, status := range statuses {
		if !activeLookup[status] {
			return false
		}
	}
	return true
}

// keepaliveInterval is how often the web server pings the batchq server
// to defer its idle-timeout. The batchq server defaults to a 1m idle
// timeout when autospawned; 30s gives us a comfortable margin.
const keepaliveInterval = 30 * time.Second

// logTailBytes caps the size of a log file returned to the browser by
// default. Bigger values balloon page weight; ?full=1 opts out.
const logTailBytes = 256 * 1024

// runKeepalive pings the batchq server periodically so its idle
// shutdown doesn't fire underneath the web UI. Returns when ctx is
// cancelled.
func (s *webServer) runKeepalive(ctx context.Context) {
	ticker := time.NewTicker(keepaliveInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			if err := s.client.Health(pingCtx); err != nil {
				s.logf("keepalive ping failed: %v", err)
			}
			cancel()
		}
	}
}

// handleJobLogs serves a job's stdout or stderr file. Streams text/plain
// with the last logTailBytes by default; ?full=1 returns everything.
func (s *webServer) handleJobLogs(w http.ResponseWriter, r *http.Request) {
	s.logf("web request %s %s", r.Method, r.URL.Path)
	jobId := r.PathValue("id")
	stream := r.PathValue("stream")
	if stream != "stdout" && stream != "stderr" {
		http.NotFound(w, r)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), webRequestTimeout)
	defer cancel()

	job, err := s.getJob(ctx, jobId)
	if err != nil {
		s.serveError(w, r, "log: job lookup", err)
		return
	}
	if job == nil {
		http.NotFound(w, r)
		return
	}

	path := resolveLogPath(job, stream)
	if path == "" {
		http.Error(w, "no "+stream+" path configured for this job", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Log-Path", path)

	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			w.Header().Set("X-Log-State", "not-yet-written")
			fmt.Fprintf(w, "(no %s file yet — job may not have started)\n\nexpected at: %s\n", stream, path)
			return
		}
		s.serveError(w, r, "log: open", err)
		return
	}
	defer f.Close()

	full := r.URL.Query().Get("full") == "1"
	info, err := f.Stat()
	if err != nil {
		s.serveError(w, r, "log: stat", err)
		return
	}
	w.Header().Set("X-Log-Size", fmt.Sprintf("%d", info.Size()))

	if !full && info.Size() > logTailBytes {
		// Seek to the tail and prepend a notice so the user knows we
		// truncated.
		if _, err := f.Seek(-logTailBytes, io.SeekEnd); err != nil {
			s.serveError(w, r, "log: seek", err)
			return
		}
		w.Header().Set("X-Log-Truncated", "1")
		fmt.Fprintf(w, "[showing last %d of %d bytes — append ?full=1 for the whole file]\n\n", logTailBytes, info.Size())
	}
	if _, err := io.Copy(w, f); err != nil {
		// Headers may already be sent; just log.
		log.Printf("web log copy %s: %v", path, err)
	}
}

// resolveLogPath returns the (possibly relative) on-disk path for the
// requested stream. %JOBID is substituted with the actual job id, and a
// relative path is anchored to the job's working directory.
func resolveLogPath(job *jobs.JobDef, stream string) string {
	raw := job.GetDetail(stream, "")
	if raw == "" {
		return ""
	}
	expanded := strings.ReplaceAll(raw, "%JOBID", job.JobId)
	if strings.HasPrefix(expanded, "/") {
		return expanded
	}
	if wd := job.GetDetail("wd", ""); wd != "" && wd != "." {
		return filepath.Join(wd, expanded)
	}
	return expanded
}
