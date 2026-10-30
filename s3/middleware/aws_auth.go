package middleware

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

// AWSCredential stores AWS access key and secret key
type AWSCredential struct {
	AccessKeyID     string
	SecretAccessKey string
	UserID          int64
}

// CredentialStore interface for storing and retrieving AWS credentials
type CredentialStore interface {
	GetCredential(accessKeyID string) (*AWSCredential, error)
	PutCredential(credential *AWSCredential) error
}

// InMemoryCredentialStore is a simple in-memory credential store
type InMemoryCredentialStore struct {
	mu          sync.RWMutex
	credentials map[string]AWSCredential
}

// NewInMemoryCredentialStore creates a new in-memory credential store
func NewInMemoryCredentialStore() *InMemoryCredentialStore {
	return &InMemoryCredentialStore{
		credentials: make(map[string]AWSCredential),
	}
}

// GetCredential retrieves a credential by access key ID
func (s *InMemoryCredentialStore) GetCredential(accessKeyID string) (*AWSCredential, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if cred, ok := s.credentials[accessKeyID]; ok {
		return &cred, nil
	}
	return nil, fmt.Errorf("credential not found")
}

// PutCredential stores a credential
func (s *InMemoryCredentialStore) PutCredential(credential *AWSCredential) error {
	if credential == nil || credential.AccessKeyID == "" || credential.SecretAccessKey == "" || credential.UserID <= 0 {
		return fmt.Errorf("invalid credential")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.credentials[credential.AccessKeyID] = *credential
	return nil
}

// Global credential store (can be replaced with database-backed implementation)
var credentialStore CredentialStore = NewInMemoryCredentialStore()
var credentialStoreMu sync.RWMutex

// SetCredentialStore sets the global credential store
func SetCredentialStore(store CredentialStore) {
	credentialStoreMu.Lock()
	defer credentialStoreMu.Unlock()
	credentialStore = store
}

type awsAuthorization struct {
	accessKeyID, date, region, service, signedHeaders, signature string
}

func parseAWSAuthorization(r *http.Request) (*awsAuthorization, error) {
	const algorithm = "AWS4-HMAC-SHA256 "
	header := r.Header.Get("Authorization")
	if !strings.HasPrefix(header, algorithm) {
		return nil, fmt.Errorf("invalid authorization header")
	}
	fields := make(map[string]string)
	for _, part := range strings.Split(strings.TrimPrefix(header, algorithm), ",") {
		pair := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(pair) != 2 || pair[1] == "" || fields[pair[0]] != "" {
			return nil, fmt.Errorf("invalid authorization fields")
		}
		fields[pair[0]] = pair[1]
	}
	scope := strings.Split(fields["Credential"], "/")
	if len(fields) != 3 || len(scope) != 5 || scope[0] == "" || scope[2] == "" || scope[3] != "s3" || scope[4] != "aws4_request" {
		return nil, fmt.Errorf("invalid credential scope")
	}
	signature, err := hex.DecodeString(fields["Signature"])
	if err != nil || len(signature) != sha256.Size {
		return nil, fmt.Errorf("invalid signature")
	}
	amzDate := r.Header.Get("X-Amz-Date")
	requestTime, err := time.Parse("20060102T150405Z", amzDate)
	if err != nil || requestTime.Format("20060102") != scope[1] {
		return nil, fmt.Errorf("invalid request date")
	}
	if skew := time.Since(requestTime); skew < -15*time.Minute || skew > 15*time.Minute {
		return nil, fmt.Errorf("request time out of range")
	}
	signedHeaders := fields["SignedHeaders"]
	names := strings.Split(signedHeaders, ";")
	signed := make(map[string]bool)
	for i, name := range names {
		if name == "" || name != strings.ToLower(name) || strings.TrimSpace(name) != name || (i > 0 && names[i-1] >= name) {
			return nil, fmt.Errorf("invalid signed headers")
		}
		if name == "host" {
			if r.Host == "" {
				return nil, fmt.Errorf("missing host")
			}
		} else if r.Header.Get(name) == "" {
			return nil, fmt.Errorf("missing signed header")
		}
		signed[name] = true
	}
	if !signed["host"] || !signed["x-amz-date"] {
		return nil, fmt.Errorf("host and x-amz-date must be signed")
	}
	// All S3 operation and metadata headers must be authenticated. The payload
	// hash is already included separately in the canonical request.
	for name := range r.Header {
		lower := strings.ToLower(name)
		if strings.HasPrefix(lower, "x-amz-") && lower != "x-amz-content-sha256" && !signed[lower] {
			return nil, fmt.Errorf("unsigned S3 header")
		}
	}
	return &awsAuthorization{scope[0], scope[1], scope[2], scope[3], signedHeaders, fields["Signature"]}, nil
}

// VerifyAWSV4Signature validates scope, timestamp, signed headers and body.
func VerifyAWSV4Signature(r *http.Request, credential *AWSCredential) error {
	auth, err := parseAWSAuthorization(r)
	if err != nil {
		return err
	}
	if credential == nil || credential.AccessKeyID != auth.accessKeyID || credential.SecretAccessKey == "" || credential.UserID <= 0 {
		return fmt.Errorf("invalid credential")
	}
	canonical, err := buildCanonicalRequest(r, auth.signedHeaders)
	if err != nil {
		return err
	}
	toSign := buildStringToSign(r.Header.Get("X-Amz-Date"), auth.date, auth.region, auth.service, canonical)
	expected, _ := hex.DecodeString(calculateSignature(credential.SecretAccessKey, auth.date, auth.region, auth.service, toSign))
	actual, _ := hex.DecodeString(auth.signature)
	if !hmac.Equal(actual, expected) {
		return fmt.Errorf("signature mismatch")
	}

	body, err := readAWSBody(r)
	if err != nil {
		return err
	}
	if payloadHash := r.Header.Get("X-Amz-Content-Sha256"); payloadHash != "" && payloadHash != "UNSIGNED-PAYLOAD" {
		expectedHash, err := hex.DecodeString(payloadHash)
		actualHash := sha256.Sum256(body)
		if err != nil || len(expectedHash) != sha256.Size || !hmac.Equal(expectedHash, actualHash[:]) {
			return fmt.Errorf("payload hash mismatch")
		}
	}
	return nil
}

// readAWSBody bounds buffering even when AuthenticateAWSV4 is used without Gin.
// It restores the body for the storage handler and propagates read failures.
func readAWSBody(r *http.Request) ([]byte, error) {
	if r.Body == nil {
		return nil, nil
	}
	limit := maxRequestBytes()
	if r.ContentLength > limit {
		return nil, &http.MaxBytesError{Limit: limit}
	}
	original := r.Body
	body, err := io.ReadAll(io.LimitReader(original, limit+1))
	original.Close()
	if err != nil {
		return nil, fmt.Errorf("read request body: %w", err)
	}
	if int64(len(body)) > limit {
		return nil, &http.MaxBytesError{Limit: limit}
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	return body, nil
}

// buildCanonicalRequest builds the canonical request string
func buildCanonicalRequest(r *http.Request, signedHeaders string) (string, error) {
	// HTTP method
	method := r.Method

	// Canonical URI (path)
	// AWS Signature V4: Use the raw path exactly as sent by the client
	// For S3, we need to use the path as-is, but URL-encode each segment
	canonicalURI := r.URL.Path
	if canonicalURI == "" {
		canonicalURI = "/"
	}
	// URL encode the path (each segment separately, preserving slashes)
	canonicalURI = urlEncodePath(canonicalURI)
	// Ensure path starts with /
	if !strings.HasPrefix(canonicalURI, "/") {
		canonicalURI = "/" + canonicalURI
	}

	// Canonical query string
	canonicalQueryString := buildCanonicalQueryString(r.URL.Query())

	// Canonical headers
	canonicalHeaders := buildCanonicalHeaders(r, signedHeaders)

	// Signed headers
	signedHeadersList := strings.ToLower(signedHeaders)

	// Payload hash (for S3, this is usually the SHA256 of the request body)
	payloadHash := r.Header.Get("X-Amz-Content-Sha256")
	if payloadHash == "" {
		body, err := readAWSBody(r)
		if err != nil {
			return "", err
		}
		sum := sha256.Sum256(body)
		payloadHash = hex.EncodeToString(sum[:])
	} else if payloadHash != "UNSIGNED-PAYLOAD" {
		decoded, err := hex.DecodeString(payloadHash)
		if err != nil || len(decoded) != sha256.Size {
			return "", fmt.Errorf("unsupported payload hash")
		}
	}

	// Build canonical request
	canonicalRequest := strings.Join([]string{
		method,
		canonicalURI,
		canonicalQueryString,
		canonicalHeaders,
		signedHeadersList,
		payloadHash,
	}, "\n")

	return canonicalRequest, nil
}

// buildCanonicalQueryString builds the canonical query string
func buildCanonicalQueryString(query url.Values) string {
	var parts []string
	for key, values := range query {
		if len(values) == 0 {
			values = []string{""}
		}
		for _, value := range values {
			parts = append(parts, awsEncode(key)+"="+awsEncode(value))
		}
	}
	sort.Strings(parts)
	return strings.Join(parts, "&")
}

func awsEncode(value string) string {
	return strings.ReplaceAll(url.QueryEscape(value), "+", "%20")
}

// buildCanonicalHeaders builds the canonical headers string
func buildCanonicalHeaders(r *http.Request, signedHeaders string) string {
	// Parse signed headers list
	headerNames := strings.Split(strings.ToLower(signedHeaders), ";")

	// Build header map (lowercase keys)
	headerMap := make(map[string]string)
	for _, name := range headerNames {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		// Get header value (case-insensitive)
		// Special handling for 'host' header - use r.Host if header is not set
		var value string
		if name == "host" {
			// Host header: use r.Host if available, otherwise use Header
			if r.Host != "" {
				value = r.Host
			} else {
				value = r.Header.Get("Host")
			}

		} else {
			value = r.Header.Get(name)
			if value == "" {
				// Try with different case
				for k, v := range r.Header {
					if strings.ToLower(k) == name {
						value = strings.Join(v, ",")
						break
					}
				}
			}
		}
		// Normalize header value according to AWS Signature V4 spec:
		// - Trim leading and trailing whitespace
		// - Convert sequential spaces to a single space
		// AWS spec says: "Remove any spaces around the colon in the header"
		// and "Convert sequential spaces to single space"
		value = strings.TrimSpace(value)
		// For all headers, collapse multiple spaces to single space
		// But preserve the structure for x-amz-* headers (they usually don't have spaces anyway)
		value = strings.Join(strings.Fields(value), " ")
		headerMap[name] = value
	}

	// Sort header names
	sort.Strings(headerNames)

	// Build canonical headers string
	parts := make([]string, 0, len(headerNames))
	for _, name := range headerNames {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if value, ok := headerMap[name]; ok {
			parts = append(parts, name+":"+value)
		}
	}

	return strings.Join(parts, "\n") + "\n"
}

// buildStringToSign builds the string to sign
func buildStringToSign(amzDate, date, region, service, canonicalRequest string) string {
	algorithm := "AWS4-HMAC-SHA256"
	credentialScope := date + "/" + region + "/" + service + "/aws4_request"

	// Hash the canonical request
	hasher := sha256.New()
	hasher.Write([]byte(canonicalRequest))
	hashedCanonicalRequest := hex.EncodeToString(hasher.Sum(nil))

	stringToSign := strings.Join([]string{
		algorithm,
		amzDate,
		credentialScope,
		hashedCanonicalRequest,
	}, "\n")

	return stringToSign
}

// calculateSignature calculates the AWS Signature V4
func calculateSignature(secretAccessKey, date, region, service, stringToSign string) string {
	// kDate = HMAC("AWS4" + SecretKey, Date)
	kDate := hmacSHA256([]byte("AWS4"+secretAccessKey), date)

	// kRegion = HMAC(kDate, Region)
	kRegion := hmacSHA256(kDate, region)

	// kService = HMAC(kRegion, Service)
	kService := hmacSHA256(kRegion, service)

	// kSigning = HMAC(kService, "aws4_request")
	kSigning := hmacSHA256(kService, "aws4_request")

	// signature = HMAC(kSigning, StringToSign)
	signature := hmacSHA256(kSigning, stringToSign)

	return hex.EncodeToString(signature)
}

// hmacSHA256 computes HMAC-SHA256
func hmacSHA256(key []byte, data string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(data))
	return mac.Sum(nil)
}

// urlEncodePath URL encodes a path (preserving forward slashes)
// AWS Signature V4 requires each path segment to be encoded, but slashes are preserved
func urlEncodePath(path string) string {
	// Handle empty path
	if path == "" {
		return "/"
	}

	// Split by slash, encode each part, then rejoin
	parts := strings.Split(path, "/")
	encodedParts := make([]string, len(parts))
	for i, part := range parts {
		if part == "" {
			encodedParts[i] = ""
		} else {
			// URL encode the part
			encodedParts[i] = url.QueryEscape(part)
			// Replace + with %20 (AWS requirement)
			encodedParts[i] = strings.ReplaceAll(encodedParts[i], "+", "%20")
		}
	}
	result := strings.Join(encodedParts, "/")
	// Ensure leading slash
	if !strings.HasPrefix(result, "/") {
		result = "/" + result
	}
	return result
}

// AuthenticateAWSV4 authenticates a request using AWS Signature V4.
func AuthenticateAWSV4(r *http.Request) (*AWSCredential, error) {
	auth, err := parseAWSAuthorization(r)
	if err != nil {
		return nil, err
	}
	credentialStoreMu.RLock()
	store := credentialStore
	credentialStoreMu.RUnlock()
	if store == nil {
		return nil, fmt.Errorf("credential store unavailable")
	}
	credential, err := store.GetCredential(auth.accessKeyID)
	if err != nil {
		return nil, fmt.Errorf("credential not found")
	}
	if err := VerifyAWSV4Signature(r, credential); err != nil {
		return nil, err
	}
	return credential, nil
}
