package main

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
)

// Test AWS Signature V4 verification logic
func main() {
	// Test case: GET /test-bucket/?location=
	// This is what warp sends during preparation phase

	secretKey := "minioadmin"
	date := "20251113"
	region := "us-east-1"
	service := "s3"
	amzDate := "20251113T082123Z"

	// Create a mock request
	req, _ := http.NewRequest("GET", "http://localhost:9000/test-bucket/?location=", nil)
	req.Header.Set("Host", "localhost:9000")
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")

	// Authorization header from warp (example format)
	// We'll build it step by step to test
	signedHeaders := "host;x-amz-content-sha256;x-amz-date"

	// Build canonical request
	canonicalRequest := buildCanonicalRequest(req, signedHeaders)
	fmt.Printf("=== Canonical Request ===\n%s\n\n", canonicalRequest)

	// Build string to sign
	stringToSign := buildStringToSign(amzDate, date, region, service, canonicalRequest)
	fmt.Printf("=== String to Sign ===\n%s\n\n", stringToSign)

	// Calculate signature
	expectedSig := calculateSignature(secretKey, date, region, service, stringToSign)
	fmt.Printf("=== Calculated Signature ===\n%s\n\n", expectedSig)

	// Test different variations to find the issue
	fmt.Println("=== Testing Variations ===")

	// Test 1: Path without trailing slash
	req2, _ := http.NewRequest("GET", "http://localhost:9000/test-bucket?location=", nil)
	req2.Header.Set("Host", "localhost:9000")
	req2.Header.Set("X-Amz-Date", amzDate)
	req2.Header.Set("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
	canonical2 := buildCanonicalRequest(req2, signedHeaders)
	fmt.Printf("Path without trailing slash:\n%s\n\n", canonical2)

	// Test 2: Different host format
	req3, _ := http.NewRequest("GET", "http://localhost:9000/test-bucket/?location=", nil)
	req3.Host = "localhost:9000"
	req3.Header.Set("X-Amz-Date", amzDate)
	req3.Header.Set("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
	canonical3 := buildCanonicalRequest(req3, signedHeaders)
	fmt.Printf("Using req.Host:\n%s\n\n", canonical3)
}

func buildCanonicalRequest(r *http.Request, signedHeaders string) string {
	method := r.Method

	// Canonical URI
	canonicalURI := r.URL.Path
	if canonicalURI == "" {
		canonicalURI = "/"
	}
	canonicalURI = urlEncodePath(canonicalURI)
	if !strings.HasPrefix(canonicalURI, "/") {
		canonicalURI = "/" + canonicalURI
	}

	// Canonical query string
	canonicalQueryString := buildCanonicalQueryString(r.URL.Query())

	// Canonical headers
	canonicalHeaders := buildCanonicalHeaders(r, signedHeaders)

	// Signed headers
	signedHeadersList := strings.ToLower(signedHeaders)

	// Payload hash
	payloadHash := r.Header.Get("X-Amz-Content-Sha256")
	if payloadHash == "" {
		if r.ContentLength == 0 {
			payloadHash = "UNSIGNED-PAYLOAD"
		} else {
			var bodyBytes []byte
			if r.Body != nil {
				bodyBytes, _ = io.ReadAll(r.Body)
				r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
			}
			if len(bodyBytes) == 0 {
				payloadHash = "UNSIGNED-PAYLOAD"
			} else {
				hasher := sha256.New()
				hasher.Write(bodyBytes)
				payloadHash = hex.EncodeToString(hasher.Sum(nil))
			}
		}
	}

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

func buildCanonicalQueryString(query url.Values) string {
	if len(query) == 0 {
		return ""
	}

	keys := make([]string, 0, len(query))
	for k := range query {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		encodedKey := url.QueryEscape(k)
		encodedKey = strings.ReplaceAll(encodedKey, "+", "%20")
		encodedKey = strings.ReplaceAll(encodedKey, "~", "%7E")

		values := query[k]
		if len(values) == 0 {
			parts = append(parts, encodedKey+"=")
		} else {
			sort.Strings(values)
			for _, v := range values {
				if v == "" {
					parts = append(parts, encodedKey+"=")
				} else {
					encodedValue := url.QueryEscape(v)
					encodedValue = strings.ReplaceAll(encodedValue, "+", "%20")
					encodedValue = strings.ReplaceAll(encodedValue, "~", "%7E")
					parts = append(parts, encodedKey+"="+encodedValue)
				}
			}
		}
	}

	return strings.Join(parts, "&")
}

func buildCanonicalHeaders(r *http.Request, signedHeaders string) string {
	headerNames := strings.Split(strings.ToLower(signedHeaders), ";")
	headerMap := make(map[string]string)

	for _, name := range headerNames {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}

		var value string
		if name == "host" {
			if r.Host != "" {
				value = r.Host
			} else {
				value = r.Header.Get("Host")
			}
			value = strings.ToLower(value)
			isHTTPS := r.URL.Scheme == "https" || strings.HasSuffix(value, ":443")
			if strings.HasSuffix(value, ":80") && !isHTTPS {
				value = strings.TrimSuffix(value, ":80")
			} else if strings.HasSuffix(value, ":443") && isHTTPS {
				value = strings.TrimSuffix(value, ":443")
			}
		} else {
			value = r.Header.Get(name)
			if value == "" {
				for k, v := range r.Header {
					if strings.ToLower(k) == name {
						value = strings.Join(v, ",")
						break
					}
				}
			}
		}

		value = strings.TrimSpace(value)
		value = strings.Join(strings.Fields(value), " ")
		headerMap[name] = value
	}

	sort.Strings(headerNames)
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

func urlEncodePath(path string) string {
	if path == "" {
		return "/"
	}
	parts := strings.Split(path, "/")
	encodedParts := make([]string, len(parts))
	for i, part := range parts {
		if part == "" {
			encodedParts[i] = ""
		} else {
			encodedParts[i] = url.QueryEscape(part)
			encodedParts[i] = strings.ReplaceAll(encodedParts[i], "+", "%20")
		}
	}
	result := strings.Join(encodedParts, "/")
	if !strings.HasPrefix(result, "/") {
		result = "/" + result
	}
	return result
}

func buildStringToSign(amzDate, date, region, service, canonicalRequest string) string {
	algorithm := "AWS4-HMAC-SHA256"
	credentialScope := date + "/" + region + "/" + service + "/aws4_request"

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

func calculateSignature(secretAccessKey, date, region, service, stringToSign string) string {
	kDate := hmacSHA256([]byte("AWS4"+secretAccessKey), date)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, service)
	kSigning := hmacSHA256(kService, "aws4_request")
	signature := hmacSHA256(kSigning, stringToSign)
	return hex.EncodeToString(signature)
}

func hmacSHA256(key []byte, data string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(data))
	return mac.Sum(nil)
}
