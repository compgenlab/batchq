package support

import (
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
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

func JoinInt(nums []int, sep string) string {
	if len(nums) == 0 {
		return ""
	}
	strs := make([]string, len(nums))

	for i, num := range nums {
		strs[i] = strconv.Itoa(num) // convert int to string
	}
	return strings.Join(strs, sep)

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
	return time.Now().Format("2006-01-02 15:04:05 UTC")
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
