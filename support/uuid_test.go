package support

import "testing"

func TestIsUUID(t *testing.T) {
	if !IsUUID(NewUUID()) {
		t.Fatal("NewUUID() output must be a valid UUID")
	}
	good := "df651630-9ec7-4c3f-966b-0a1b050ae4e7"
	if !IsUUID(good) {
		t.Fatalf("IsUUID(%q) = false, want true", good)
	}
	for _, bad := range []string{
		"",
		"not-a-uuid",
		"df651630-9ec7-4c3f-966b-0a1b050ae4e",   // too short
		"df651630-9ec7-4c3f-966b-0a1b050ae4e77", // too long
		"df651630x9ec7-4c3f-966b-0a1b050ae4e7",  // wrong separator
		"gf651630-9ec7-4c3f-966b-0a1b050ae4e7",  // non-hex char
	} {
		if IsUUID(bad) {
			t.Errorf("IsUUID(%q) = true, want false", bad)
		}
	}
}
