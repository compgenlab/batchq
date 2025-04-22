package iniconfig

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
)

type Config struct {
	sections []string
	data     map[string]map[string]string
}

func LoadConfig(filename string, defsection string) *Config {
	return LoadConfigEnv(filename, defsection, "")
}

func LoadConfigEnv(filename string, defsection string, prefix string) *Config {
	cfg := &Config{sections: []string{}, data: make(map[string]map[string]string)}

	f, err := os.Open(filename)
	if err == nil {
		// no config file to load, but we can still return the object (will load defaults or ENV)
		defer f.Close()

		section := defsection
		scanner := bufio.NewScanner(f)
		regNonWord := regexp.MustCompile(`[^A-Za-z0-9_\.]+`)

		for scanner.Scan() {
			line := scanner.Text()

			// remove comments from line
			if strings.Contains(line, "#") {
				spl := strings.SplitN(line, "#", 2)
				line = spl[0]
			}

			// remove any leading/trailing whitespace
			line = strings.TrimSpace(line)

			// skip blank lines
			if len(line) == 0 || line == "" {
				continue
			}

			if line[0] == '[' {
				// any line that starts with '[' should be
				// a section.
				if line[len(line)-1] == ']' {
					line = line[1 : len(line)-1]
					if regNonWord.Match([]byte(line)) {
						fmt.Printf("Invalid line: %s\n", line)
					} else {
						section = strings.TrimSpace(line)
					}
					// section = regNonWord.ReplaceAllString(strings.TrimSpace(section), ".")
				} else {
					// not a section... :(
					fmt.Printf("Invalid line: %s\n", line)
				}
			} else {
				// this is a value in the current section
				spl := strings.SplitN(line, "=", 2)
				k := strings.TrimSpace(spl[0])
				v := strings.TrimSpace(spl[1])
				if len(spl) > 1 {
					found := false
					for _, val := range cfg.sections {
						if val == section {
							found = true
						}
					}
					if !found {
						cfg.sections = append(cfg.sections, section)
						cfg.data[section] = make(map[string]string)
					}
					// fmt.Printf("[%s] %s => %s\n", section, k, v)
					cfg.data[section][k] = v
				} else {
					fmt.Printf("Missing value: %s\n", k)
				}
			}
		}
	}

	if prefix != "" {
		// Load ENV values
		// Replace _ with . in key names.
		//
		// PREFIX_DBTYPE=pg
		// PREFIX_PG_HOST=localhost
		//
		//  is the same as:
		//
		// dbtype=pg
		// [pg]
		// host=localhost
		//

	}

	return cfg
}

func (c Config) Sections() []string {
	return c.sections
}

func (c Config) Get(section string, key string, defval ...string) (string, bool) {
	if _, ok := c.data[section]; ok {
		if _, ok2 := c.data[section][key]; ok2 {
			return c.data[section][key], true
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
		v = strings.ToUpper(v)
		if v == "TRUE" || v == "T" {
			return true, true
		}
		if v == "FALSE" || v == "F" {
			return false, true
		}
	}

	if len(defval) > 0 {
		return defval[0], true
	}

	return false, false
}
