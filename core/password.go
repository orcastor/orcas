package core

import (
	"crypto/rand"
	"crypto/sha1" // Only for verifying existing password hashes.
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"

	"golang.org/x/crypto/pbkdf2"
)

const passwordIterations = 600000

// HashPassword uses PBKDF2-HMAC-SHA256 and an independently random salt.
func HashPassword(password string) (string, error) {
	if password == "" {
		return "", fmt.Errorf("password must not be empty")
	}
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	dk := pbkdf2.Key([]byte(password), salt, passwordIterations, 32, sha256.New)
	return fmt.Sprintf("pbkdf2-sha256:%d:%s:%s", passwordIterations,
		base64.URLEncoding.EncodeToString(salt), base64.URLEncoding.EncodeToString(dk)), nil
}

// verifyPassword bounds work and validates all fields before key derivation.
// Three-field SHA1 hashes remain readable and are upgraded after login.
func verifyPassword(password, encoded string) (valid, legacy bool) {
	parts := strings.Split(encoded, ":")
	keyLen, hashFunc := 32, sha256.New
	if len(parts) == 3 {
		legacy, keyLen, hashFunc = true, 16, sha1.New
	} else if len(parts) == 4 && parts[0] == "pbkdf2-sha256" {
		parts = parts[1:]
	} else {
		return false, false
	}
	iterations, err := strconv.Atoi(parts[0])
	if err != nil || iterations < 1 || iterations > 1000000 {
		return false, false
	}
	salt, err := base64.URLEncoding.DecodeString(parts[1])
	if err != nil || len(salt) != 16 {
		return false, false
	}
	want, err := base64.URLEncoding.DecodeString(parts[2])
	if err != nil || len(want) != keyLen {
		return false, false
	}
	got := pbkdf2.Key([]byte(password), salt, iterations, keyLen, hashFunc)
	return subtle.ConstantTimeCompare(got, want) == 1, legacy
}
