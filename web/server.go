package web

import (
	"bytes"
	"context"
	"embed"
	"errors"
	"fmt"
	"html/template"
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

	"github.com/mbreese/batchq/db"
	"github.com/mbreese/batchq/iniconfig"
	"github.com/mbreese/batchq/jobs"
	"github.com/mbreese/batchq/support"
)

//go:embed templates/*.html
var webTemplatesFS embed.FS

const webRequestTimeout = 2 * time.Minute

type Options struct {
	Config     *iniconfig.Config
	DBPath     string
	SocketPath string
	ListenAddr string
	Force      bool
	Verbose    bool
}

type webServer struct {
	db        db.BatchDB
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

type searchPage struct {
	Title           string
	ContentTemplate string
	Query           string
	Results         []*jobs.JobDef
	StatusOptions   []string
	SelectedStatus  map[string]bool
}

func StartServer(opts Options) error {
	log.SetOutput(os.Stderr)
	socketPath, err := resolveSocketPath(opts)
	if err != nil {
		return err
	}
	listener, kind, address, cleanup, err := createListener(socketPath, resolveListenAddress(opts), opts.Force)
	if err != nil {
		return err
	}
	defer cleanup()

	if strings.TrimSpace(opts.DBPath) == "" {
		return errors.New("db path required")
	}
	templates, err := loadWebTemplates()
	if err != nil {
		return err
	}

	jobq, err := db.OpenDB(opts.DBPath)
	if err != nil {
		return err
	}
	defer jobq.Close()

	server := &webServer{db: jobq, templates: templates, verbose: opts.Verbose}
	mux := http.NewServeMux()
	mux.HandleFunc("/jobs/", server.handleJob)
	mux.HandleFunc("/jobs", server.handleQueue)
	mux.HandleFunc("/search", server.handleSearch)
	mux.HandleFunc("/", server.handleQueue)

	httpServer := &http.Server{
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	shutdownCh := make(chan os.Signal, 1)
	signal.Notify(shutdownCh, os.Interrupt, syscall.SIGTERM)

	go func() {
		if opts.Verbose {
			log.Printf("batchq web listening on %s: %s", kind, address)
		}
		if err := httpServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("web server error: %v", err)
		}
	}()

	<-shutdownCh
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return httpServer.Shutdown(ctx)
}

func resolveSocketPath(opts Options) (string, error) {
	if opts.SocketPath != "" {
		return normalizeSocketPath(opts.SocketPath)
	}

	home := iniconfig.GetBatchqHome()
	defaultSocket := filepath.Join(home, "batchq.sock")
	if opts.Config == nil {
		return normalizeSocketPath(defaultSocket)
	}
	socket, _ := opts.Config.Get("batchq", "web_socket", defaultSocket)
	return normalizeSocketPath(socket)
}

func resolveListenAddress(opts Options) string {
	if opts.ListenAddr != "" {
		return opts.ListenAddr
	}
	if opts.Config == nil {
		return ""
	}
	if val, ok := opts.Config.Get("batchq", "web_listen"); ok {
		return val
	}
	return ""
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
	jobList := s.jobsForFilter(ctx, statuses)
	showAll := len(statuses) == 0
	data := queuePage{
		Title:           "batchq queue",
		ContentTemplate: "queue-content",
		Jobs:            jobList,
		ShowAll:         showAll,
		Query:           "",
		StatusOptions:   statusOptions(),
		SelectedStatus:  selected,
	}

	s.render(w, r, data)
}

func (s *webServer) handleJob(w http.ResponseWriter, r *http.Request) {
	s.logf("web request %s %s", r.Method, r.URL.Path)
	jobId := strings.TrimPrefix(r.URL.Path, "/jobs/")
	if jobId == "" || strings.Contains(jobId, "/") {
		http.NotFound(w, r)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), webRequestTimeout)
	defer cancel()

	job := s.db.GetJob(ctx, jobId)
	if job == nil {
		http.NotFound(w, r)
		return
	}

	parents := fetchJobs(ctx, s.db, job.AfterOk)
	children := fetchJobs(ctx, s.db, s.db.GetJobDependents(ctx, job.JobId))

	detailRows, script := buildDetailRows(job.Details, "script")
	runningRows, slurmScript := buildDetailRowsRunning(job.RunningDetails, "slurm_script")
	var parentTree *jobTreeNode
	if len(parents) > 0 {
		parentTree = buildParentTree(ctx, s.db, job.JobId, map[string]bool{})
	}
	var childTree *jobTreeNode
	if len(children) > 0 {
		childTree = buildChildTree(ctx, s.db, job.JobId, map[string]bool{})
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

func (s *webServer) handleSearch(w http.ResponseWriter, r *http.Request) {
	s.logf("web request %s %s", r.Method, r.URL.Path)
	if r.URL.Path != "/search" {
		http.NotFound(w, r)
		return
	}

	query := strings.TrimSpace(r.URL.Query().Get("q"))

	ctx, cancel := context.WithTimeout(r.Context(), webRequestTimeout)
	defer cancel()

	statuses, selected := parseStatusFilter(r)
	var results []*jobs.JobDef
	if query != "" {
		if len(statuses) > 0 {
			results = s.db.SearchJobs(ctx, query, statuses)
		} else {
			results = s.db.SearchJobs(ctx, query, nil)
		}
	}

	data := searchPage{
		Title:           "batchq search",
		ContentTemplate: "search-content",
		Query:           query,
		Results:         results,
		StatusOptions:   statusOptions(),
		SelectedStatus:  selected,
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

func (s *webServer) logf(format string, args ...any) {
	if s.verbose {
		log.Printf(format, args...)
	}
}

func fetchJobs(ctx context.Context, batchdb db.BatchDB, ids []string) []*jobs.JobDef {
	var list []*jobs.JobDef
	for _, id := range ids {
		if job := batchdb.GetJob(ctx, id); job != nil {
			list = append(list, job)
		}
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].JobId < list[j].JobId
	})
	return list
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

func buildParentTree(ctx context.Context, batchdb db.BatchDB, jobId string, visited map[string]bool) *jobTreeNode {
	job := batchdb.GetJob(ctx, jobId)
	if job == nil {
		return nil
	}
	node := &jobTreeNode{Job: job}
	if visited[jobId] {
		return node
	}
	visited[jobId] = true
	defer delete(visited, jobId)
	for _, parentId := range job.AfterOk {
		if parent := buildParentTree(ctx, batchdb, parentId, visited); parent != nil {
			node.Children = append(node.Children, parent)
		}
	}
	sort.Slice(node.Children, func(i, j int) bool {
		return node.Children[i].Job.JobId < node.Children[j].Job.JobId
	})
	return node
}

func buildChildTree(ctx context.Context, batchdb db.BatchDB, jobId string, visited map[string]bool) *jobTreeNode {
	job := batchdb.GetJob(ctx, jobId)
	if job == nil {
		return nil
	}
	node := &jobTreeNode{Job: job}
	if visited[jobId] {
		return node
	}
	visited[jobId] = true
	defer delete(visited, jobId)
	childIds := batchdb.GetJobDependents(ctx, jobId)
	for _, childId := range childIds {
		if child := buildChildTree(ctx, batchdb, childId, visited); child != nil {
			node.Children = append(node.Children, child)
		}
	}
	sort.Slice(node.Children, func(i, j int) bool {
		return node.Children[i].Job.JobId < node.Children[j].Job.JobId
	})
	return node
}

func (s *webServer) jobsForFilter(ctx context.Context, statuses []jobs.StatusCode) []*jobs.JobDef {
	if len(statuses) == 0 {
		return s.db.GetQueueJobs(ctx, true, true)
	}
	if isActiveStatusSet(statuses) {
		return s.db.GetQueueJobs(ctx, false, true)
	}
	return s.db.GetJobsByStatus(ctx, statuses, true)
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
