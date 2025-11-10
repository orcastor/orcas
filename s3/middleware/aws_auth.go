package middleware

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
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
	credentials map[string]*AWSCredential
}

// NewInMemoryCredentialStore creates a new in-memory credential store
func NewInMemoryCredentialStore() *InMemoryCredentialStore {
	return &InMemoryCredentialStore{
		credentials: make(map[string]*AWSCredential),
	}
}

// GetCredential retrieves a credential by access key ID
func (s *InMemoryCredentialStore) GetCredential(accessKeyID string) (*AWSCredential, error) {
	if cred, ok := s.credentials[accessKeyID]; ok {
		return cred, nil
	}
	return nil, fmt.Errorf("credential not found")
}

// PutCredential stores a credential
func (s *InMemoryCredentialStore) PutCredential(credential *AWSCredential) error {
	s.credentials[credential.AccessKeyID] = credential
	return nil
}

// Global credential store (can be replaced with database-backed implementation)
var credentialStore CredentialStore = NewInMemoryCredentialStore()

// SetCredentialStore sets the global credential store
func SetCredentialStore(store CredentialStore) {
	credentialStore = store
}

// VerifyAWSV4Signature verifies AWS Signature Version 4
func VerifyAWSV4Signature(r *http.Request, credential *AWSCredential) error {
	// Parse Authorization header
	authHeader := r.Header.Get("Authorization")
	if !strings.HasPrefix(authHeader, "AWS4-HMAC-SHA256 ") {
		return fmt.Errorf("invalid authorization header format")
	}

	// Extract components from Authorization header
	// Format: AWS4-HMAC-SHA256 Credential=accessKeyId/date/region/service/aws4_request, SignedHeaders=..., Signature=...
	authParts := strings.TrimPrefix(authHeader, "AWS4-HMAC-SHA256 ")

	var accessKeyID, date, region, service, signedHeaders, signature string

	// Parse Credential
	credentialStart := strings.Index(authParts, "Credential=")
	if credentialStart == -1 {
		return fmt.Errorf("missing Credential in authorization header")
	}
	credentialEnd := strings.Index(authParts[credentialStart:], ",")
	if credentialEnd == -1 {
		return fmt.Errorf("invalid Credential format")
	}
	credentialStr := authParts[credentialStart+11 : credentialStart+credentialEnd]
	credParts := strings.Split(credentialStr, "/")
	if len(credParts) != 5 {
		return fmt.Errorf("invalid Credential format")
	}
	accessKeyID = credParts[0]
	date = credParts[1]
	region = credParts[2]
	service = credParts[3]

	// Verify access key ID matches
	if accessKeyID != credential.AccessKeyID {
		return fmt.Errorf("access key ID mismatch")
	}

	// Parse SignedHeaders
	signedHeadersStart := strings.Index(authParts, "SignedHeaders=")
	if signedHeadersStart == -1 {
		return fmt.Errorf("missing SignedHeaders in authorization header")
	}
	signedHeadersEnd := strings.Index(authParts[signedHeadersStart:], ",")
	if signedHeadersEnd == -1 {
		signedHeadersEnd = len(authParts) - signedHeadersStart
	}
	signedHeaders = authParts[signedHeadersStart+14 : signedHeadersStart+signedHeadersEnd]

	// Parse Signature
	signatureStart := strings.Index(authParts, "Signature=")
	if signatureStart == -1 {
		return fmt.Errorf("missing Signature in authorization header")
	}
	signature = authParts[signatureStart+10:]

	// Get X-Amz-Date header (required for V4)
	amzDate := r.Header.Get("X-Amz-Date")
	if amzDate == "" {
		return fmt.Errorf("missing X-Amz-Date header")
	}

	// Verify date matches
	if len(amzDate) >= 8 && amzDate[:8] != date {
		return fmt.Errorf("date mismatch")
	}

	// Build canonical request
	canonicalRequest := buildCanonicalRequest(r, signedHeaders)

	// Build string to sign
	stringToSign := buildStringToSign(amzDate, date, region, service, canonicalRequest)

	// Calculate signature
	expectedSignature := calculateSignature(credential.SecretAccessKey, date, region, service, stringToSign)

	// Compare signatures
	if signature != expectedSignature {
		return fmt.Errorf("signature mismatch")
	}

	return nil
}

// buildCanonicalRequest builds the canonical request string
func buildCanonicalRequest(r *http.Request, signedHeaders string) string {
	// HTTP method
	method := r.Method

	// Canonical URI (path)
	canonicalURI := r.URL.Path
	if canonicalURI == "" {
		canonicalURI = "/"
	}
	// URL encode the path
	canonicalURI = urlEncodePath(canonicalURI)

	// Canonical query string
	canonicalQueryString := buildCanonicalQueryString(r.URL.Query())

	// Canonical headers
	canonicalHeaders := buildCanonicalHeaders(r, signedHeaders)

	// Signed headers
	signedHeadersList := strings.ToLower(signedHeaders)

	// Payload hash (for S3, this is usually the SHA256 of the request body)
	payloadHash := r.Header.Get("X-Amz-Content-Sha256")
	if payloadHash == "" {
		// If no payload hash header, calculate from body
		// For simplicity, we'll use "UNSIGNED-PAYLOAD" for requests without body
		if r.ContentLength == 0 {
			payloadHash = "UNSIGNED-PAYLOAD"
		} else {
			// In a real implementation, you'd need to read and hash the body
			// For now, we'll assume it's provided in the header
			payloadHash = "UNSIGNED-PAYLOAD"
		}
	}

	// Build canonical request
	canonicalRequest := strings.Join([]string{
		method,
		canonicalURI,
		canonicalQueryString,
		canonicalHeaders,
		"",
		signedHeadersList,
		payloadHash,
	}, "\n")

	return canonicalRequest
}

// buildCanonicalQueryString builds the canonical query string
func buildCanonicalQueryString(query url.Values) string {
	if len(query) == 0 {
		return ""
	}

	// Sort query parameters by name
	keys := make([]string, 0, len(query))
	for k := range query {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Build canonical query string
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		// URL encode key
		encodedKey := url.QueryEscape(k)
		encodedKey = strings.ReplaceAll(encodedKey, "+", "%20")

		// Get values
		values := query[k]
		if len(values) == 0 {
			parts = append(parts, encodedKey+"=")
		} else {
			// Sort values if multiple
			sort.Strings(values)
			for _, v := range values {
				encodedValue := url.QueryEscape(v)
				encodedValue = strings.ReplaceAll(encodedValue, "+", "%20")
				parts = append(parts, encodedKey+"="+encodedValue)
			}
		}
	}

	return strings.Join(parts, "&")
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
		value := r.Header.Get(name)
		if value == "" {
			// Try with different case
			for k, v := range r.Header {
				if strings.ToLower(k) == name {
					value = strings.Join(v, ",")
					break
				}
			}
		}
		// Normalize header value (trim spaces, collapse multiple spaces)
		value = strings.TrimSpace(value)
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
func urlEncodePath(path string) string {
	parts := strings.Split(path, "/")
	encodedParts := make([]string, len(parts))
	for i, part := range parts {
		encodedParts[i] = url.QueryEscape(part)
		encodedParts[i] = strings.ReplaceAll(encodedParts[i], "+", "%20")
	}
	return strings.Join(encodedParts, "/")
}

// AuthenticateAWSV4 authenticates a request using AWS Signature V4
func AuthenticateAWSV4(r *http.Request) (*AWSCredential, error) {
	// Parse Authorization header to get access key ID
	authHeader := r.Header.Get("Authorization")
	if !strings.HasPrefix(authHeader, "AWS4-HMAC-SHA256 ") {
		return nil, fmt.Errorf("invalid authorization header")
	}

	// Extract access key ID from Credential
	credentialStart := strings.Index(authHeader, "Credential=")
	if credentialStart == -1 {
		return nil, fmt.Errorf("missing Credential in authorization header")
	}
	credentialEnd := strings.Index(authHeader[credentialStart:], ",")
	if credentialEnd == -1 {
		return nil, fmt.Errorf("invalid Credential format")
	}
	credentialStr := authHeader[credentialStart+11 : credentialStart+credentialEnd]
	credParts := strings.Split(credentialStr, "/")
	if len(credParts) < 1 {
		return nil, fmt.Errorf("invalid Credential format")
	}
	accessKeyID := credParts[0]

	// Get credential from store
	credential, err := credentialStore.GetCredential(accessKeyID)
	if err != nil {
		return nil, fmt.Errorf("credential not found: %v", err)
	}

	// Verify signature
	if err := VerifyAWSV4Signature(r, credential); err != nil {
		return nil, fmt.Errorf("signature verification failed: %v", err)
	}

	// Check request time (prevent replay attacks)
	amzDate := r.Header.Get("X-Amz-Date")
	if amzDate != "" {
		// Parse date (format: YYYYMMDDTHHMMSSZ)
		if len(amzDate) >= 15 {
			requestTime, err := time.Parse("20060102T150405Z", amzDate[:15])
			if err == nil {
				// Allow 15 minutes clock skew
				now := time.Now().UTC()
				diff := now.Sub(requestTime)
				if diff < -15*time.Minute || diff > 15*time.Minute {
					return nil, fmt.Errorf("request time out of range")
				}
			}
		}
	}

	return credential, nil
}
