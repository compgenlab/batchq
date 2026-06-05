package support

import "testing"

// TestConfigCloneDeepCopiesResources guards the PR #30 fix: Clone must deep-copy
// the resource maps so mutating the clone (e.g. ApplyDefaults on the resolved
// config) can't corrupt the retained raw snapshot the debug command reads.
func TestConfigCloneDeepCopiesResources(t *testing.T) {
	orig := &Config{}
	orig.JobDefaults.Resources = map[string]string{"gpu": "1"}
	orig.SimpleRunner.Resources = map[string]string{"cluster": "a"}
	orig.SlurmRunner.Resources = map[string]string{"partition": "p"}

	clone := orig.Clone()

	// Mutate every map on the clone.
	clone.JobDefaults.Resources["gpu"] = "2"
	clone.JobDefaults.Resources["new"] = "x"
	clone.SimpleRunner.Resources["cluster"] = "b"
	clone.SlurmRunner.Resources["partition"] = "q"

	if orig.JobDefaults.Resources["gpu"] != "1" {
		t.Errorf("JobDefaults.Resources[gpu] = %q, want unchanged \"1\"", orig.JobDefaults.Resources["gpu"])
	}
	if _, ok := orig.JobDefaults.Resources["new"]; ok {
		t.Error("JobDefaults.Resources gained a key from the clone")
	}
	if orig.SimpleRunner.Resources["cluster"] != "a" {
		t.Errorf("SimpleRunner.Resources[cluster] = %q, want unchanged \"a\"", orig.SimpleRunner.Resources["cluster"])
	}
	if orig.SlurmRunner.Resources["partition"] != "p" {
		t.Errorf("SlurmRunner.Resources[partition] = %q, want unchanged \"p\"", orig.SlurmRunner.Resources["partition"])
	}
}

// TestConfigCloneNil guards the nil-receiver shortcut.
func TestConfigCloneNil(t *testing.T) {
	var c *Config
	if c.Clone() != nil {
		t.Error("(*Config)(nil).Clone() should be nil")
	}
}
