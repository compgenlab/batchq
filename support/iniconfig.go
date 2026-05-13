package support

// iniconfig.go is a small INI-style config loader used by batchq for the
// `~/.batchq/config` file. It's intentionally tiny — sections, key=value
// lines, `#` comments — and tolerant of a missing file so callers can
// fall back to defaults without checking.

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
)

// Config is an in-memory representation of an INI file with insertion-order
// section iteration.
type Config struct {
	sections []string
	data     map[string]map[string]string
}

// LoadConfig reads an INI file from filename. Missing files yield an empty
// Config (not an error) so callers can rely on built-in defaults when no
// config is installed. defsection names the section that keys above the
// first `[section]` header belong to.
func LoadConfig(filename string, defsection string) *Config {
	cfg := &Config{sections: []string{}, data: make(map[string]map[string]string)}

	f, err := os.Open(filename)
	if err != nil {
		return cfg
	}
	defer f.Close()

	section := defsection
	scanner := bufio.NewScanner(f)
	regNonWord := regexp.MustCompile(`[^A-Za-z0-9_\.]+`)

	for scanner.Scan() {
		line := scanner.Text()
		if i := strings.Index(line, "#"); i >= 0 {
			line = line[:i]
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if line[0] == '[' {
			if line[len(line)-1] != ']' {
				fmt.Printf("Invalid line: %s\n", line)
				continue
			}
			name := line[1 : len(line)-1]
			if regNonWord.Match([]byte(name)) {
				fmt.Printf("Invalid line: %s\n", line)
				continue
			}
			section = strings.TrimSpace(name)
			continue
		}
		spl := strings.SplitN(line, "=", 2)
		if len(spl) < 2 {
			fmt.Printf("Missing value: %s\n", strings.TrimSpace(spl[0]))
			continue
		}
		k := strings.TrimSpace(spl[0])
		v := strings.TrimSpace(spl[1])
		if _, ok := cfg.data[section]; !ok {
			cfg.sections = append(cfg.sections, section)
			cfg.data[section] = make(map[string]string)
		}
		cfg.data[section][k] = v
	}
	return cfg
}

func (c Config) Sections() []string {
	return c.sections
}

func (c Config) Get(section string, key string, defval ...string) (string, bool) {
	if sec, ok := c.data[section]; ok {
		if v, ok := sec[key]; ok {
			return v, true
		}
	}
	if len(defval) > 0 {
		return defval[0], true
	}
	return "", false
}

func (c Config) GetInt(section string, key string, defval ...int) (int, bool) {
	if v, ok := c.Get(section, key); ok {
		if i, err := strconv.Atoi(v); err == nil {
			return i, true
		}
	}
	if len(defval) > 0 {
		return defval[0], true
	}
	return -1, false
}

func (c Config) GetBool(section string, key string, defval ...bool) (bool, bool) {
	if v, ok := c.Get(section, key); ok {
		switch strings.ToUpper(v) {
		case "TRUE", "T":
			return true, true
		case "FALSE", "F":
			return false, true
		}
	}
	if len(defval) > 0 {
		return defval[0], true
	}
	return false, false
}
