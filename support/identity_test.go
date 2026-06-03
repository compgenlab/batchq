package support

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
)

// fakeNSS returns a stub runCommand that answers a fixed set of
// (cmd, args...) tuples. Anything not in the table errors out.
func fakeNSS(t *testing.T, table map[string]string) func() {
	t.Helper()
	return SwapRunCommandForTest(func(name string, args ...string) ([]byte, int, error) {
		key := name
		for _, a := range args {
			key += " " + a
		}
		out, ok := table[key]
		if !ok {
			return nil, 1, fmt.Errorf("fakeNSS: unexpected command %q", key)
		}
		return []byte(out), 0, nil
	})
}

func TestLookupUserByUid_HappyPath(t *testing.T) {
	defer fakeNSS(t, map[string]string{
		"getent passwd 1234": "alice:x:1234:100:Alice:/home/alice:/bin/bash\n",
		"id -G alice":        "100 4 24 1000\n",
	})()

	ident, err := LookupUserByUid(1234)
	if err != nil {
		t.Fatalf("LookupUserByUid: %v", err)
	}
	want := UserIdentity{
		Username: "alice",
		Uid:      1234,
		Gid:      100,
		Groups:   []uint32{100, 4, 24, 1000},
	}
	if !reflect.DeepEqual(ident, want) {
		t.Fatalf("identity: got %+v, want %+v", ident, want)
	}
}

func TestLookupUserByUid_UnknownUidIsErrUserNotFound(t *testing.T) {
	defer SwapRunCommandForTest(func(name string, args ...string) ([]byte, int, error) {
		// getent passwd <unknown> exits 2 with empty stdout.
		return nil, 2, fmt.Errorf("getent passwd: no entry")
	})()

	_, err := LookupUserByUid(99999)
	if !errors.Is(err, ErrUserNotFound) {
		t.Fatalf("err: got %v, want ErrUserNotFound", err)
	}
}

func TestLookupUserByUid_GroupsLookupFailureKeepsPrimary(t *testing.T) {
	calls := 0
	defer SwapRunCommandForTest(func(name string, args ...string) ([]byte, int, error) {
		calls++
		switch calls {
		case 1: // getent passwd
			return []byte("bob:x:2000:50:Bob:/home/bob:/bin/sh\n"), 0, nil
		case 2: // id -G
			return []byte("id: bob: NSS hiccup\n"), 1, fmt.Errorf("exit 1")
		}
		return nil, 1, fmt.Errorf("unexpected call %d", calls)
	})()

	ident, err := LookupUserByUid(2000)
	if err == nil {
		t.Fatalf("expected groups-lookup error, got nil")
	}
	// Primary identity should still be populated despite the groups failure.
	if ident.Username != "bob" || ident.Uid != 2000 || ident.Gid != 50 {
		t.Fatalf("partial identity: %+v", ident)
	}
	if len(ident.Groups) != 0 {
		t.Fatalf("groups should be empty on failure, got %v", ident.Groups)
	}
}

func TestParsePasswdLine(t *testing.T) {
	name, gid, err := parsePasswdLine([]byte("carol:x:3001:42:Carol:/home/c:/bin/zsh\n"))
	if err != nil {
		t.Fatalf("parsePasswdLine: %v", err)
	}
	if name != "carol" || gid != 42 {
		t.Fatalf("got (%q, %d)", name, gid)
	}
}

func TestParsePasswdLine_Malformed(t *testing.T) {
	if _, _, err := parsePasswdLine([]byte("not a passwd line")); err == nil {
		t.Fatal("expected error on malformed line")
	}
}
