package support

import (
	"crypto/rand"
	"math/big"
)

// secretAlphabet is the character set for generated secrets: alphanumeric, no
// ambiguous punctuation, safe to paste into a shell, an Authorization header,
// or a URL.
const secretAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"

// RandomString returns a cryptographically random alphanumeric string of length
// n. It draws unbiased indices from crypto/rand; an error means the system CSPRNG
// is unavailable, in which case the caller must NOT fall back to a weak secret.
func RandomString(n int) (string, error) {
	b := make([]byte, n)
	max := big.NewInt(int64(len(secretAlphabet)))
	for i := range b {
		idx, err := rand.Int(rand.Reader, max)
		if err != nil {
			return "", err
		}
		b[i] = secretAlphabet[idx.Int64()]
	}
	return string(b), nil
}

// RandomToken returns a "sk-"-prefixed random token (24 random chars), the
// format used for the web gateway's REST API bearer token.
func RandomToken() (string, error) {
	s, err := RandomString(24)
	if err != nil {
		return "", err
	}
	return "sk-" + s, nil
}
