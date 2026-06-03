package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/mbreese/batchq/api"
	"github.com/mbreese/batchq/internal/testsupport"
	"github.com/mbreese/batchq/service"
	"github.com/mbreese/batchq/storage"
	"github.com/mbreese/batchq/support"
)

// End-to-end check that the server's ConnContext callback populates
// peer creds for unix-socket requests, and that SubmitJob then
// overrides the job's uid/gid with the kernel-attested values.
//
// We run a real server on a real unix socket and dial it with a real
// http.Client. The NSS shellout is stubbed so the test doesn't
// depend on whatever /etc/passwd looks like on the host.
func TestSubmitOverPeerCredsDerivesIdentity(t *testing.T) {
	uid := uint32(os.Getuid())
	gid := uint32(os.Getgid())
	username := fmt.Sprintf("u%d", uid)

	defer support.SwapRunCommandForTest(func(name string, args ...string) ([]byte, int, error) {
		switch name {
		case "getent":
			if len(args) == 2 && args[0] == "passwd" && args[1] == strconv.FormatUint(uint64(uid), 10) {
				return []byte(fmt.Sprintf("%s:x:%d:%d::/h:/bin/sh\n", username, uid, gid)), 0, nil
			}
		case "id":
			if len(args) == 2 && args[0] == "-G" && args[1] == username {
				return []byte(fmt.Sprintf("%d 4 24\n", gid)), 0, nil
			}
		}
		return nil, 1, fmt.Errorf("unexpected command: %s %v", name, args)
	})()

	// Bring up a real server on a unix socket.
	dir := testsupport.ShortSockDir(t)
	sock := filepath.Join(dir, "peer.sock")
	dbPath := filepath.Join(t.TempDir(), "peer.db")
	st, err := storage.Open(context.Background(), dbPath, storage.Options{})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	srv, err := New(service.New(st), Options{Listen: "unix://" + sock})
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	waitForSocket(t, sock, 2*time.Second)

	// Build an http.Client that dials our unix socket.
	httpClient := &http.Client{
		Transport: &http.Transport{
			DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
				return net.Dial("unix", sock)
			},
		},
		Timeout: 5 * time.Second,
	}

	// The client lies about uid/gid; the server must ignore it.
	reqBody, _ := json.Marshal(api.SubmitJobRequest{
		Details: map[string]string{
			"script": "echo hi",
			"uid":    "9999",
			"gid":    "9999",
			"groups": "9999",
		},
	})
	resp, err := httpClient.Post("http://unix"+api.Prefix+api.RouteJobs, "application/json", bytes.NewReader(reqBody))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status %d: %s", resp.StatusCode, body)
	}

	var sr api.SubmitJobResponse
	if err := json.NewDecoder(resp.Body).Decode(&sr); err != nil {
		t.Fatalf("decode: %v", err)
	}

	wantUid := strconv.FormatUint(uint64(uid), 10)
	wantGid := strconv.FormatUint(uint64(gid), 10)
	if sr.Job.Details["uid"] != wantUid {
		t.Fatalf("uid: got %q, want %q", sr.Job.Details["uid"], wantUid)
	}
	if sr.Job.Details["gid"] != wantGid {
		t.Fatalf("gid: got %q, want %q", sr.Job.Details["gid"], wantGid)
	}
	if sr.Job.Details["groups"] == "" || sr.Job.Details["groups"] == "9999" {
		t.Fatalf("groups: got %q, expected NSS-derived list", sr.Job.Details["groups"])
	}
}
