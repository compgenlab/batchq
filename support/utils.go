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

func AmIRoot() bool {
	if u, err := user.Current(); err == nil {
		return u.Uid == "0"
	}
	return false
}

func ExpandPathAbs(path string) (string, error) {
	if path == "" {
		return "", nil
	}
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

func Contains[T comparable](slice []T, val T) bool {
	for _, v := range slice {
		if v == val {
			return true
		}
	}
	return false
}

// FileExists reports whether path exists and is statable. A stat error other
// than "not found" (e.g. a permission error) reports false rather than
// masquerading as existence.
func FileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
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
