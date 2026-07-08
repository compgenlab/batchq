package support

import (
	"strings"
	"testing"
)

func TestRandomString(t *testing.T) {
	const n = 16
	seen := map[string]bool{}
	for i := 0; i < 100; i++ {
		s, err := RandomString(n)
		if err != nil {
			t.Fatalf("RandomString: %v", err)
		}
		if len(s) != n {
			t.Fatalf("len = %d, want %d", len(s), n)
		}
		for _, r := range s {
			if !strings.ContainsRune(secretAlphabet, r) {
				t.Fatalf("char %q not in alphabet", r)
			}
		}
		if seen[s] {
			t.Fatalf("duplicate string %q in 100 draws", s)
		}
		seen[s] = true
	}
}

func TestRandomToken(t *testing.T) {
	tok, err := RandomToken()
	if err != nil {
		t.Fatalf("RandomToken: %v", err)
	}
	if !strings.HasPrefix(tok, "sk-") {
		t.Fatalf("token %q missing sk- prefix", tok)
	}
	if len(tok) != len("sk-")+24 {
		t.Fatalf("token len = %d, want %d", len(tok), len("sk-")+24)
	}
}
