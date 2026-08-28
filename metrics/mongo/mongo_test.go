package mongo

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/logger"
)

type tlsFixture struct {
	caPEM       []byte
	clientPEM   []byte
	clientKeyPEM []byte
	serverCert  tls.Certificate
}

func newTLSFixture(t *testing.T) tlsFixture {
	t.Helper()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}

	f := tlsFixture{caPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})}

	serverKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serverTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "mongo.test"},
		DNSNames:     []string{"mongo.test"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	serverDER, err := x509.CreateCertificate(rand.Reader, serverTemplate, caCert, &serverKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	serverPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverDER})
	serverKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: mustMarshalECKey(t, serverKey)})
	if f.serverCert, err = tls.X509KeyPair(serverPEM, serverKeyPEM); err != nil {
		t.Fatal(err)
	}

	clientKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	clientTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: "test-client"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	clientDER, err := x509.CreateCertificate(rand.Reader, clientTemplate, caCert, &clientKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	f.clientPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientDER})
	f.clientKeyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: mustMarshalECKey(t, clientKey)})

	return f
}

func mustMarshalECKey(t *testing.T, key *ecdsa.PrivateKey) []byte {
	t.Helper()
	der, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return der
}

func startTLSListener(t *testing.T, serverCfg *tls.Config) (addr string, stateCh <-chan tls.ConnectionState) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	st := make(chan tls.ConnectionState, 1)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			tc := tls.Server(conn, serverCfg)
			if err := tc.Handshake(); err != nil {
				_ = conn.Close()
				continue
			}
			st <- tc.ConnectionState()
		}
	}()
	return ln.Addr().String(), st
}

func TestNewInvalidTLSParam(t *testing.T) {
	c, err := New("127.0.0.1:27017", "", "", "",
		common.TLSCredentials{}, map[string]string{"tls": "banana"},
		time.Minute, 10*time.Second, logger.NewKlog("test"), nil, "test", 0, false)
	if err == nil {
		t.Fatal("expected an error for an invalid tls param")
	}
	if c != nil {
		t.Error("collector must be nil on error")
	}
}

func TestNewInvalidCAFailsClosed(t *testing.T) {
	c, err := New("127.0.0.1:27017", "", "", "",
		common.TLSCredentials{CA: "not a pem"}, map[string]string{"tls": "true"},
		time.Minute, 10*time.Second, logger.NewKlog("test"), nil, "test", 0, false)
	if err == nil {
		t.Fatal("expected an error for an invalid CA")
	}
	if c != nil {
		t.Error("collector must be nil on error: no plaintext fallback allowed")
	}
}

func TestNewInvalidClientCertFailsClosed(t *testing.T) {
	f := newTLSFixture(t)
	badKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	badKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: mustMarshalECKey(t, badKey)})

	c, err := New("127.0.0.1:27017", "", "", "",
		common.TLSCredentials{CA: string(f.caPEM), Cert: string(f.clientPEM), Key: string(badKeyPEM)},
		map[string]string{"tls": "true"},
		time.Minute, 10*time.Second, logger.NewKlog("test"), nil, "test", 0, false)
	if err == nil {
		t.Fatal("expected an error for an invalid client certificate")
	}
	if c != nil {
		t.Error("collector must be nil on error: no plaintext fallback allowed")
	}
}

func TestNewWithoutTLSSucceeds(t *testing.T) {
	c, err := New("127.0.0.1:27017", "", "", "",
		common.TLSCredentials{}, nil,
		time.Hour, 10*time.Second, logger.NewKlog("test"), nil, "test", 0, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c == nil {
		t.Fatal("collector must not be nil")
	}
	_ = c.Close()
}

// The collector must connect over TLS, send the configured SNI, and trust
// only the configured CA. The MongoDB wire protocol never needs to succeed:
// the TLS handshake itself is what this test verifies.
func TestNewTLSHandshakeSendsConfiguredSNI(t *testing.T) {
	f := newTLSFixture(t)
	serverCfg := &tls.Config{Certificates: []tls.Certificate{f.serverCert}}
	addr, stateCh := startTLSListener(t, serverCfg)

	c, err := New(addr, "", "", "mongo.test",
		common.TLSCredentials{CA: string(f.caPEM)}, map[string]string{"tls": "true"},
		time.Hour, 5*time.Second, logger.NewKlog("test"), nil, "test", 0, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = c.Close() }()

	select {
	case st := <-stateCh:
		if st.ServerName != "mongo.test" {
			t.Errorf("server saw SNI %q, want %q", st.ServerName, "mongo.test")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the TLS handshake")
	}
}

// With no SNI configured (pod-discovered targets are scraped by IP), the
// handshake must still succeed via chain-only verification.
func TestNewTLSHandshakeWithoutSNIVerifiesChainOnly(t *testing.T) {
	f := newTLSFixture(t)
	serverCfg := &tls.Config{Certificates: []tls.Certificate{f.serverCert}}
	addr, stateCh := startTLSListener(t, serverCfg)

	c, err := New(addr, "", "", "",
		common.TLSCredentials{CA: string(f.caPEM)}, map[string]string{"tls": "true"},
		time.Hour, 5*time.Second, logger.NewKlog("test"), nil, "test", 0, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = c.Close() }()

	select {
	case st := <-stateCh:
		if st.ServerName != "" {
			t.Errorf("server saw SNI %q, want empty (no SNI must be synthesized)", st.ServerName)
		}
		// Reaching this point means the client completed the handshake,
		// i.e. its chain-only VerifyConnection accepted the server chain.
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the TLS handshake")
	}
}
