package support

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"time"
)

func GetUserHomeFilePath(path ...string) (string, error) {
	home := os.Getenv("HOME")
	if home == "" {
		usr, err := user.Current()
		if err != nil {
			return "", err
		}
		home = usr.HomeDir
	}

	tmp := make([]string, len(path)+1)
	tmp[0] = home
	for i := 0; i < len(path); i++ {
		tmp[i+1] = path[i]
	}
	return filepath.Join(tmp...), nil
}

func JoinStrings(vals []string, sep string) string {
	if len(vals) == 0 {
		return ""
	}
	return strings.Join(vals, sep)
}

func AmIRoot() bool {
	if u, err := user.Current(); err == nil {
		return u.Uid == "0"
	}
	return false
}

func GetCurrentUsername() string {
	u, err := user.Current()
	if err == nil {
		return u.Username
	}
	if name := os.Getenv("USER"); name != "" {
		return name
	}
	if name := os.Getenv("USERNAME"); name != "" {
		return name
	}
	return "unknown"
}

func ExpandPathAbs(path string) (string, error) {
	endsWithSlash := false
	if path[len(path)-1] == os.PathSeparator {
		endsWithSlash = true
	}
	if strings.HasPrefix(path, "~") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		path = filepath.Join(home, path[1:])
	}
	ret, err := filepath.Abs(path)
	if endsWithSlash {
		ret = ret + string(os.PathSeparator)
	}
	return ret, err
}

func GetNowUTCString() string {
	return time.Now().UTC().Format("2006-01-02 15:04:05 MST")
}

func MustWriteFile(path, content string) {
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		panic(fmt.Sprintf("Failed to write to %s: %v", path, err))
	}
}

func Contains[T comparable](slice []T, val T) bool {
	for _, v := range slice {
		if v == val {
			return true
		}
	}
	return false
}

func FileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil || !os.IsNotExist(err)
}

func NewUUID() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		panic(fmt.Sprintf("failed to generate uuid: %v", err))
	}

	// Version 4 (random) UUID.
	buf[6] = (buf[6] & 0x0f) | 0x40
	buf[8] = (buf[8] & 0x3f) | 0x80

	hexStr := hex.EncodeToString(buf)
	return fmt.Sprintf("%s-%s-%s-%s-%s", hexStr[0:8], hexStr[8:12], hexStr[12:16], hexStr[16:20], hexStr[20:32])
}
