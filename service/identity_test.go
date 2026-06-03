package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/mbreese/batchq/api"
	"github.com/mbreese/batchq/support"
)

// ctxWithPeer attaches peer creds to the test's standard context.
func ctxWithPeer(t *testing.T, uid, gid uint32) context.Context {
	t.Helper()
	return support.WithPeerCreds(ctxT(t), support.PeerCreds{Uid: uid, Gid: gid})
}

// fakeNSS installs a stub runCommand that answers the table; anything
// else errors. Used so submit and authz tests don't depend on the
// host's actual /etc/passwd or NSS configuration.
func fakeNSS(t *testing.T, table map[string]string) {
	t.Helper()
	t.Cleanup(support.SwapRunCommandForTest(func(name string, args ...string) ([]byte, int, error) {
		key := name
		for _, a := range args {
			key += " " + a
		}
		out, ok := table[key]
		if !ok {
			return nil, 1, fmt.Errorf("fakeNSS: unexpected command %q", key)
		}
		return []byte(out), 0, nil
	}))
}

// SubmitJob over a unix socket (peer creds present) MUST ignore the
// client-supplied uid/gid and write the kernel-attested ones — plus
// the NSS-derived supplementary groups.
func TestSubmitJob_DerivesIdentityFromPeerCreds(t *testing.T) {
	fakeNSS(t, map[string]string{
		"getent passwd 1234": "alice:x:1234:100:Alice:/home/alice:/bin/bash\n",
		"id -G alice":        "100 4 24 1000\n",
	})
	svc := newService(t)

	ctx := ctxWithPeer(t, 1234, 100)
	dto, err := svc.SubmitJob(ctx, &api.SubmitJobRequest{
		Details: map[string]string{
			"script": "echo hi",
			"uid":    "9999", // client lying — must be ignored
			"gid":    "9999",
			"groups": "9999", // must be replaced with NSS supp groups
		},
	})
	if err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}

	if dto.Details["uid"] != "1234" {
		t.Fatalf("uid: got %q, want %q", dto.Details["uid"], "1234")
	}
	if dto.Details["gid"] != "100" {
		t.Fatalf("gid: got %q, want %q", dto.Details["gid"], "100")
	}
	if dto.Details["groups"] != "100,4,24,1000" {
		t.Fatalf("groups: got %q, want %q", dto.Details["groups"], "100,4,24,1000")
	}
}

// Without peer creds (e.g. a remote/proxy client) the server must
// preserve today's behavior: client-supplied identity is trusted.
// This will tighten when bearer-token validation lands.
func TestSubmitJob_NoPeerCredsPreservesClientIdentity(t *testing.T) {
	svc := newService(t)
	dto, err := svc.SubmitJob(ctxT(t), &api.SubmitJobRequest{
		Details: map[string]string{
			"script": "echo hi",
			"uid":    "9999",
			"gid":    "8888",
		},
	})
	if err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}
	if dto.Details["uid"] != "9999" || dto.Details["gid"] != "8888" {
		t.Fatalf("identity not preserved: %+v", dto.Details)
	}
	if _, ok := dto.Details["groups"]; ok {
		t.Fatalf("groups should not be synthesized without peer creds: %+v", dto.Details)
	}
}

// If NSS can't resolve the peer's uid (ErrUserNotFound), submit still
// succeeds and the peer-attested uid/gid are written; only the
// supplementary groups are skipped.
func TestSubmitJob_NSSUnknownUidStillTrustsPeer(t *testing.T) {
	t.Cleanup(support.SwapRunCommandForTest(func(name string, args ...string) ([]byte, int, error) {
		return nil, 2, fmt.Errorf("getent passwd: no entry")
	}))
	svc := newService(t)

	dto, err := svc.SubmitJob(ctxWithPeer(t, 4242, 4242), &api.SubmitJobRequest{
		Details: map[string]string{"script": "echo", "uid": "9999"},
	})
	if err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}
	if dto.Details["uid"] != "4242" {
		t.Fatalf("uid: got %q, want 4242", dto.Details["uid"])
	}
	if dto.Details["gid"] != "4242" {
		t.Fatalf("gid: got %q, want 4242", dto.Details["gid"])
	}
	if _, ok := dto.Details["groups"]; ok {
		t.Fatalf("groups should be unset on NSS miss: %+v", dto.Details)
	}
}

// --- Authz on hold/release/cancel -------------------------------------

// submitAs is a tiny helper that submits a job through a peer-creds
// context so the resulting job carries kernel-attested ownership.
func submitAs(t *testing.T, svc *Service, uid, gid uint32) string {
	t.Helper()
	fakeNSS(t, map[string]string{
		fmt.Sprintf("getent passwd %d", uid): fmt.Sprintf("u%d:x:%d:%d::/h:/bin/sh\n", uid, uid, gid),
		fmt.Sprintf("id -G u%d", uid):        fmt.Sprintf("%d\n", gid),
	})
	dto, err := svc.SubmitJob(ctxWithPeer(t, uid, gid),
		&api.SubmitJobRequest{Details: map[string]string{"script": "echo"}})
	if err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}
	return dto.JobID
}

func TestAuthz_OwnerCanHoldReleaseCancel(t *testing.T) {
	svc := newService(t)
	jobID := submitAs(t, svc, 1234, 100)

	ctx := ctxWithPeer(t, 1234, 100)
	if err := svc.HoldJob(ctx, jobID); err != nil {
		t.Fatalf("HoldJob (owner): %v", err)
	}
	if err := svc.ReleaseJob(ctx, jobID); err != nil {
		t.Fatalf("ReleaseJob (owner): %v", err)
	}
	if err := svc.CancelJob(ctx, jobID, "test"); err != nil {
		t.Fatalf("CancelJob (owner): %v", err)
	}
}

func TestAuthz_NonOwnerIsForbidden(t *testing.T) {
	svc := newService(t)
	jobID := submitAs(t, svc, 1234, 100)

	ctx := ctxWithPeer(t, 9999, 9999)
	for _, op := range []struct {
		name string
		fn   func() error
	}{
		{"hold", func() error { return svc.HoldJob(ctx, jobID) }},
		{"release", func() error { return svc.ReleaseJob(ctx, jobID) }},
		{"cancel", func() error { return svc.CancelJob(ctx, jobID, "x") }},
	} {
		if err := op.fn(); !errors.Is(err, ErrForbidden) {
			t.Fatalf("%s: got %v, want ErrForbidden", op.name, err)
		}
	}
}

func TestAuthz_RootBypasses(t *testing.T) {
	svc := newService(t)
	jobID := submitAs(t, svc, 1234, 100)

	ctx := ctxWithPeer(t, 0, 0)
	if err := svc.HoldJob(ctx, jobID); err != nil {
		t.Fatalf("HoldJob (root): %v", err)
	}
	if err := svc.ReleaseJob(ctx, jobID); err != nil {
		t.Fatalf("ReleaseJob (root): %v", err)
	}
	if err := svc.CancelJob(ctx, jobID, "x"); err != nil {
		t.Fatalf("CancelJob (root): %v", err)
	}
}

func TestAuthz_NoPeerCredsAllows(t *testing.T) {
	// No peer creds = remote/proxy client today = allow. Will tighten
	// once bearer-token validation lands.
	svc := newService(t)
	dto, err := svc.SubmitJob(ctxT(t),
		&api.SubmitJobRequest{Details: map[string]string{"script": "echo", "uid": "1234", "gid": "100"}})
	if err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}
	if err := svc.HoldJob(ctxT(t), dto.JobID); err != nil {
		t.Fatalf("HoldJob (no peer): %v", err)
	}
}

// A job written before this change has no uid detail at all. Authz
// against it under peer-cred enforcement must fail closed; root is
// the only path through.
func TestAuthz_JobWithoutUidIsForbiddenForNonRoot(t *testing.T) {
	svc := newService(t)
	// Submit without peer creds so no uid is injected.
	dto, err := svc.SubmitJob(ctxT(t),
		&api.SubmitJobRequest{Details: map[string]string{"script": "echo"}})
	if err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}
	// And the dto has no uid detail — sanity check.
	if _, ok := dto.Details["uid"]; ok {
		t.Fatalf("test setup wrong: dto has uid: %+v", dto.Details)
	}

	err = svc.HoldJob(ctxWithPeer(t, 1234, 100), dto.JobID)
	if !errors.Is(err, ErrForbidden) {
		t.Fatalf("got %v, want ErrForbidden", err)
	}
	if !strings.Contains(err.Error(), "uid detail") {
		t.Fatalf("error should mention uid detail: %v", err)
	}
}
