package common

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
)

type testCA struct {
	caPEM  []byte
	cert   *x509.Certificate
	key    *ecdsa.PrivateKey
	clientCertPEM []byte
	clientKeyPEM  []byte
}

func newTestCA(t *testing.T) *testCA {
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

	clientKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	clientTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
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

	return &testCA{
		caPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}),
		cert:  caCert,
		key:   caKey,
		clientCertPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientDER}),
		clientKeyPEM:  pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: mustMarshalECKey(t, clientKey)}),
	}
}

// serverCertPEM returns a leaf certificate for the given DNS names, signed by the CA.
func (ca *testCA) serverCertPEM(t *testing.T, dnsNames []string) (certPEM, keyPEM []byte) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: dnsNames[0]},
		DNSNames:     dnsNames,
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: mustMarshalECKey(t, key)})
}

func mustMarshalECKey(t *testing.T, key *ecdsa.PrivateKey) []byte {
	t.Helper()
	der, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return der
}

func mustTLSConfig(t *testing.T, creds TLSCredentials, insecureSkipVerify bool) *tls.Config {
	t.Helper()
	cfg, err := DatabaseTLSConfig(creds, insecureSkipVerify)
	if err != nil {
		t.Fatalf("DatabaseTLSConfig(%+v): unexpected error: %v", creds, err)
	}
	return cfg
}

func startTestTLSServer(t *testing.T, serverCfg *tls.Config) (addr string, stateCh <-chan tls.ConnectionState, errCh <-chan error) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	st := make(chan tls.ConnectionState, 1)
	er := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			er <- err
			return
		}
		defer conn.Close()
		tc := tls.Server(conn, serverCfg)
		if err := tc.Handshake(); err != nil {
			er <- err
			return
		}
		st <- tc.ConnectionState()
	}()
	return ln.Addr().String(), st, er
}

func clientHandshake(addr string, clientCfg *tls.Config) error {
	d := net.Dialer{Timeout: 5 * time.Second}
	conn, err := tls.DialWithDialer(&d, "tcp", addr, clientCfg)
	if err != nil {
		return err
	}
	return conn.Close()
}

func TestDatabaseTLSConfigInvalidCA(t *testing.T) {
	cfg, err := DatabaseTLSConfig(TLSCredentials{CA: "not a pem"}, false)
	if err == nil {
		t.Fatal("expected error for invalid CA")
	}
	if cfg != nil {
		t.Error("config must be nil on error")
	}
}

func TestDatabaseTLSConfigInvalidClientCert(t *testing.T) {
	ca := newTestCA(t)
	otherKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	otherKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: mustMarshalECKey(t, otherKey)})

	cfg, err := DatabaseTLSConfig(TLSCredentials{CA: string(ca.caPEM), Cert: string(ca.clientCertPEM), Key: string(otherKeyPEM)}, false)
	if err == nil {
		t.Fatal("expected error for mismatched client cert/key")
	}
	if cfg != nil {
		t.Error("config must be nil on error")
	}
}

func TestDatabaseTLSConfigSkipVerify(t *testing.T) {
	cfg := mustTLSConfig(t, TLSCredentials{}, true)
	if !cfg.InsecureSkipVerify {
		t.Error("expected InsecureSkipVerify")
	}
	if cfg.VerifyConnection != nil {
		t.Error("skip-verify must not install VerifyConnection")
	}
}

// The CA is trusted as a chain only: hostname verification is intentionally
// skipped because instances are scraped by IP (see DatabaseTLSConfig).
func TestDatabaseTLSConfigChainOnlyVerification(t *testing.T) {
	ca := newTestCA(t)
	certPEM, keyPEM := ca.serverCertPEM(t, []string{"mongo.test"})
	serverCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	serverCfg := &tls.Config{Certificates: []tls.Certificate{serverCert}}

	addr, _, errCh := startTestTLSServer(t, serverCfg)
	// Server name that is NOT on the server certificate: the chain check
	// must still pass because hostname verification is skipped.
	cfg := mustTLSConfig(t, TLSCredentials{CA: string(ca.caPEM)}, false)
	cfg.ServerName = "unrelated.example.com"
	if err := clientHandshake(addr, cfg); err != nil {
		select {
		case e := <-errCh:
			t.Fatalf("chain-only handshake failed: server: %v, client: %v", e, err)
		default:
		}
		t.Fatalf("chain-only handshake failed: %v", err)
	}
}

func TestDatabaseTLSConfigRejectsUntrustedChain(t *testing.T) {
	ca := newTestCA(t)
	otherCA := newTestCA(t)
	certPEM, keyPEM := ca.serverCertPEM(t, []string{"mongo.test"})
	serverCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}

	addr, _, _ := startTestTLSServer(t, &tls.Config{Certificates: []tls.Certificate{serverCert}})
	cfg := mustTLSConfig(t, TLSCredentials{CA: string(otherCA.caPEM)}, false)
	if err := clientHandshake(addr, cfg); err == nil {
		t.Fatal("handshake with a certificate from an untrusted CA must fail")
	}
}

func TestDatabaseTLSConfigMTLS(t *testing.T) {
	ca := newTestCA(t)
	certPEM, keyPEM := ca.serverCertPEM(t, []string{"mongo.test"})
	serverCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(ca.caPEM) {
		t.Fatal("failed to load CA pool")
	}
	serverCfg := &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    caPool,
	}

	addr, stateCh, errCh := startTestTLSServer(t, serverCfg)
	cfg := mustTLSConfig(t, TLSCredentials{CA: string(ca.caPEM), Cert: string(ca.clientCertPEM), Key: string(ca.clientKeyPEM)}, false)
	cfg.ServerName = "mongo.test"
	if err := clientHandshake(addr, cfg); err != nil {
		select {
		case e := <-errCh:
			t.Fatalf("mTLS handshake failed: server: %v, client: %v", e, err)
		default:
		}
		t.Fatalf("mTLS handshake failed: %v", err)
	}
	select {
	case st := <-stateCh:
		if len(st.PeerCertificates) == 0 {
			t.Error("server did not receive a client certificate")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for server handshake state")
	}
}
