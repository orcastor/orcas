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

// Debug tool for AWS Signature V4 verification
// This tool helps identify issues in signature calculation by comparing
// different implementations and variations

func main() {
	fmt.Println("=== AWS Signature V4 Debug Tool ===\n")

	secretKey := "minioadmin"
	date := "20251113"
	region := "us-east-1"
	service := "s3"
	amzDate := "20251113T082123Z"

	// Test case from actual error: GET /test-bucket/?location=
	testCases := []struct {
		name string
		url  string
		host string
	}{
		{
			name: "With trailing slash and query",
			url:  "http://localhost:9000/test-bucket/?location=",
			host: "localhost:9000",
		},
		{
			name: "Without trailing slash",
			url:  "http://localhost:9000/test-bucket?location=",
			host: "localhost:9000",
		},
		{
			name: "Root path",
			url:  "http://localhost:9000/?location=",
			host: "localhost:9000",
		},
	}

	for i, tc := range testCases {
		fmt.Printf("--- Test Case %d: %s ---\n", i+1, tc.name)

		req, _ := http.NewRequest("GET", tc.url, nil)
		req.Host = tc.host
		req.Header.Set("X-Amz-Date", amzDate)
		req.Header.Set("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")

		signedHeaders := "host;x-amz-content-sha256;x-amz-date"

		// Build canonical request
		canonicalRequest := buildCanonicalRequest(req, signedHeaders)

		// Show components
		parts := strings.Split(canonicalRequest, "\n")
		if len(parts) >= 7 {
			fmt.Printf("Method: %s\n", parts[0])
			fmt.Printf("URI: %s\n", parts[1])
			fmt.Printf("Query: %s\n", parts[2])
			fmt.Printf("Headers:\n%s", parts[3])
			fmt.Printf("Signed Headers: %s\n", parts[5])
			fmt.Printf("Payload Hash: %s\n", parts[6])
		}

		// Build string to sign
		stringToSign := buildStringToSign(amzDate, date, region, service, canonicalRequest)

		// Calculate signature
		signature := calculateSignature(secretKey, date, region, service, stringToSign)

		fmt.Printf("\nCanonical Request Hash: %s\n", hashString(canonicalRequest))
		fmt.Printf("Signature: %s\n\n", signature)
	}

	// Test with actual error data
	fmt.Println("=== Testing with Actual Error Data ===")
	testActualError()
}

func testActualError() {
	// From error message:
	// canonical: "GET\n/test-bucket/\nlocation=\nhost:localhost:9000\nx-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\nx-amz-date:20251113T082123Z\n\n\nhost;x-amz-content-sha256;x-amz-date\ne3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

	canonicalFromError := `GET
/test-bucket/
location=
host:localhost:9000
x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
x-amz-date:20251113T082123Z


host;x-amz-content-sha256;x-amz-date
e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`

	secretKey := "minioadmin"
	date := "20251113"
	region := "us-east-1"
	service := "s3"
	amzDate := "20251113T082123Z"

	// Calculate what signature should be
	stringToSign := buildStringToSign(amzDate, date, region, service, canonicalFromError)
	expectedSig := calculateSignature(secretKey, date, region, service, stringToSign)

	fmt.Printf("Canonical Request (from error):\n%s\n\n", canonicalFromError)
	fmt.Printf("String to Sign:\n%s\n\n", stringToSign)
	fmt.Printf("Expected Signature: %s\n", expectedSig)

	// Now try to recreate the request and see if we get the same canonical
	req, _ := http.NewRequest("GET", "http://localhost:9000/test-bucket/?location=", nil)
	req.Host = "localhost:9000"
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")

	signedHeaders := "host;x-amz-content-sha256;x-amz-date"
	ourCanonical := buildCanonicalRequest(req, signedHeaders)

	fmt.Printf("\nOur Canonical Request:\n%s\n\n", ourCanonical)

	if canonicalFromError == ourCanonical {
		fmt.Println("✅ Canonical requests MATCH!")
	} else {
		fmt.Println("❌ Canonical requests DO NOT MATCH!")
		fmt.Println("\nDifferences:")
		compareCanonical(canonicalFromError, ourCanonical)
	}

	ourSig := calculateSignature(secretKey, date, region, service, buildStringToSign(amzDate, date, region, service, ourCanonical))
	fmt.Printf("\nOur Signature: %s\n", ourSig)
	if expectedSig == ourSig {
		fmt.Println("✅ Signatures MATCH!")
	} else {
		fmt.Printf("❌ Signatures DO NOT MATCH! (Expected: %s)\n", expectedSig)
	}
}

func compareCanonical(expected, actual string) {
	expectedLines := strings.Split(expected, "\n")
	actualLines := strings.Split(actual, "\n")

	maxLen := len(expectedLines)
	if len(actualLines) > maxLen {
		maxLen = len(actualLines)
	}

	for i := 0; i < maxLen; i++ {
		var exp, act string
		if i < len(expectedLines) {
			exp = expectedLines[i]
		}
		if i < len(actualLines) {
			act = actualLines[i]
		}
		if exp != act {
			fmt.Printf("Line %d: Expected: %q, Got: %q\n", i+1, exp, act)
		}
	}
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

func hashString(s string) string {
	hasher := sha256.New()
	hasher.Write([]byte(s))
	return hex.EncodeToString(hasher.Sum(nil))
}

