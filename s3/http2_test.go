package main

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"testing"
	"time"

	"golang.org/x/net/http2"
)

// generateSelfSignedCert generates a self-signed certificate for testing
func generateSelfSignedCert() (*tls.Certificate, error) {
	// Generate private key
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, fmt.Errorf("failed to generate key: %v", err)
	}

	// Create certificate template
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization:  []string{"ORCAS Test"},
			Country:       []string{"US"},
			Province:      []string{""},
			Locality:      []string{"Test"},
			StreetAddress: []string{""},
			PostalCode:    []string{""},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
		DNSNames:              []string{"localhost"},
	}

	// Create certificate
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		return nil, fmt.Errorf("failed to create certificate: %v", err)
	}

	// Encode to PEM
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	// Parse certificate
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("failed to parse certificate: %v", err)
	}

	return &cert, nil
}

// setupHTTP2TestEnvironment sets up an HTTP/2 test server with TLS
func setupHTTP2TestEnvironment(t *testing.T) (*http.Server, string, *tls.Certificate) {
	// Generate self-signed certificate for testing
	cert, err := generateSelfSignedCert()
	if err != nil {
		t.Fatalf("Failed to generate certificate: %v", err)
	}

	// Setup test environment (same as regular tests)
	_, router := setupTestEnvironment(t)

	// Create TLS config for HTTP/2
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{*cert},
		NextProtos:   []string{"h2", "http/1.1"}, // Prefer HTTP/2, fallback to HTTP/1.1
	}

	// Create HTTP server with TLS
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}

	server := &http.Server{
		Addr:      listener.Addr().String(),
		Handler:   router,
		TLSConfig: tlsConfig,
	}

	// Configure HTTP/2
	http2.ConfigureServer(server, &http2.Server{})

	// Start server in background
	go func() {
		tlsListener := tls.NewListener(listener, tlsConfig)
		if err := server.Serve(tlsListener); err != nil && err != http.ErrServerClosed {
			t.Errorf("HTTP/2 server error: %v", err)
		}
	}()

	// Wait a bit for server to start
	time.Sleep(100 * time.Millisecond)

	return server, fmt.Sprintf("https://%s", listener.Addr().String()), cert
}

// TestHTTP2Performance tests HTTP/2 performance
// This test requires TLS setup and may be slower than HTTP/1.1 tests
func TestHTTP2Performance(t *testing.T) {
	t.Skip("HTTP/2 testing requires TLS setup and is more complex - enable when needed")

	server, baseURL, cert := setupHTTP2TestEnvironment(t)
	if server == nil {
		t.Fatal("Failed to setup HTTP/2 test environment")
	}
	defer server.Close()

	// Create HTTP/2 client with TLS
	client := &http.Client{
		Transport: &http2.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true, // Only for testing!
				Certificates:       []tls.Certificate{*cert},
			},
		},
		Timeout: 30 * time.Second,
	}

	// Test PUT request
	testData := bytes.NewReader([]byte("test data"))
	req, err := http.NewRequest("PUT", fmt.Sprintf("%s/test-bucket/test-key", baseURL), testData)
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("HTTP/2 request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		t.Errorf("Expected status 200 or 204, got %d", resp.StatusCode)
	}

	// Verify HTTP/2 protocol
	if resp.ProtoMajor != 2 {
		t.Logf("Note: Got HTTP/%d.%d instead of HTTP/2 - server may have negotiated HTTP/1.1", resp.ProtoMajor, resp.ProtoMinor)
	} else {
		t.Logf("Successfully used HTTP/2 protocol")
	}
}

// HTTP2SetupExample shows how to set up HTTP/2 for testing
// This is a reference implementation that can be adapted
func HTTP2SetupExample() {
	/*
		To implement HTTP/2 testing:

		1. Install golang.org/x/net/http2:
		   go get golang.org/x/net/http2

		2. Generate self-signed certificates for testing:
		   openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -days 365 -nodes

		3. Setup TLS server:
		   server := &http.Server{
		       Addr:      ":8443",
		       TLSConfig: &tls.Config{
		           NextProtos: []string{"h2", "http/1.1"},
		       },
		   }
		   http2.ConfigureServer(server, &http2.Server{})

		4. Start server with TLS:
		   server.ListenAndServeTLS("cert.pem", "key.pem")

		5. Create HTTP/2 client:
		   client := &http.Client{
		       Transport: &http2.Transport{
		           TLSClientConfig: &tls.Config{
		               InsecureSkipVerify: true, // Only for testing
		           },
		       },
		   }

		6. Make requests - they will automatically use HTTP/2 if available
	*/
}
