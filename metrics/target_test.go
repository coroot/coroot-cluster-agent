package metrics

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	"github.com/coroot/coroot-cluster-agent/k8s"
	"github.com/coroot/logger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test PostgreSQL configurations
func TestTargetFromPod_Postgres_NoTLS(t *testing.T) {
	pod := &k8s.Pod{
		Id: k8s.PodId{
			Namespace: "test-ns",
			Name:      "test-pod",
		},
		IP: "10.0.0.1",
		Annotations: map[string]string{
			"coroot.com/postgres-scrape": "true",
			"coroot.com/postgres-scrape-credentials-username": "user",
			"coroot.com/postgres-scrape-credentials-password": "pass",
		},
	}

	target := TargetFromPod(pod)
	require.NotNil(t, target)
	assert.Equal(t, TargetTypePostgres, target.Type)
	assert.Equal(t, "10.0.0.1:5432", target.Addr)
	assert.Equal(t, "", target.Params["sslmode"])
}

func TestTargetFromPod_Postgres_VerifyFull(t *testing.T) {
	pod := &k8s.Pod{
		Id: k8s.PodId{
			Namespace: "test-ns",
			Name:      "test-pod",
		},
		IP: "10.0.0.1",
		Annotations: map[string]string{
			"coroot.com/postgres-scrape": "true",
			"coroot.com/postgres-scrape-param-sslmode": "verify-full",
			"coroot.com/postgres-scrape-credentials-secret-name": "creds",
			"coroot.com/postgres-scrape-credentials-secret-username-key": "username",
			"coroot.com/postgres-scrape-credentials-secret-password-key": "password",
		},
	}

	target := TargetFromPod(pod)
	require.NotNil(t, target)
	assert.Equal(t, "verify-full", target.Params["sslmode"])
	assert.Equal(t, "creds", target.CredentialsSecret.Name)
}

// Test MongoDB configurations
func TestTargetFromPod_MongoDB_NoTLS(t *testing.T) {
	pod := &k8s.Pod{
		Id: k8s.PodId{
			Namespace: "test-ns",
			Name:      "test-pod",
		},
		IP: "10.0.0.1",
		Annotations: map[string]string{
			"coroot.com/mongodb-scrape": "true",
			"coroot.com/mongodb-scrape-port": "27017",
			"coroot.com/mongodb-scrape-credentials-username": "user",
			"coroot.com/mongodb-scrape-credentials-password": "pass",
		},
	}

	target := TargetFromPod(pod)
	require.NotNil(t, target)
	assert.Equal(t, TargetTypeMongodb, target.Type)
	assert.Equal(t, "10.0.0.1:27017", target.Addr)
	assert.Equal(t, "", target.Params["sslmode"])
}

func TestTargetFromPod_MongoDB_Require(t *testing.T) {
	pod := &k8s.Pod{
		Id: k8s.PodId{
			Namespace: "test-ns",
			Name:      "test-pod",
		},
		IP: "10.0.0.1",
		Annotations: map[string]string{
			"coroot.com/mongodb-scrape": "true",
			"coroot.com/mongodb-scrape-param-sslmode": "require",
			"coroot.com/mongodb-scrape-credentials-secret-name": "creds",
			"coroot.com/mongodb-scrape-credentials-secret-username-key": "username",
			"coroot.com/mongodb-scrape-credentials-secret-password-key": "password",
		},
	}

	target := TargetFromPod(pod)
	require.NotNil(t, target)
	assert.Equal(t, "require", target.Params["sslmode"])
	assert.Equal(t, "creds", target.CredentialsSecret.Name)
}

func TestTargetFromPod_MongoDB_VerifyCA(t *testing.T) {
	pod := &k8s.Pod{
		Id: k8s.PodId{
			Namespace: "test-ns",
			Name:      "test-pod",
		},
		IP: "10.0.0.1",
		Annotations: map[string]string{
			"coroot.com/mongodb-scrape": "true",
			"coroot.com/mongodb-scrape-param-sslmode": "verify-ca",
			"coroot.com/mongodb-scrape-credentials-secret-name": "creds",
			"coroot.com/mongodb-scrape-credentials-secret-username-key": "username",
			"coroot.com/mongodb-scrape-credentials-secret-password-key": "password",
			"coroot.com/mongodb-scrape-tls-secret-name": "tls-secret",
			"coroot.com/mongodb-scrape-tls-secret-ca-key": "ca.crt",
		},
	}

	target := TargetFromPod(pod)
	require.NotNil(t, target)
	assert.Equal(t, "verify-ca", target.Params["sslmode"])
	assert.Equal(t, "tls-secret", target.TlsSecret.Name)
	assert.Equal(t, "ca.crt", target.TlsSecret.CaKey)
	assert.Equal(t, "test-ns", target.TlsSecret.Namespace)
}

func TestTargetFromPod_MongoDB_VerifyFull(t *testing.T) {
	pod := &k8s.Pod{
		Id: k8s.PodId{
			Namespace: "test-ns",
			Name:      "test-pod",
		},
		IP: "10.0.0.1",
		Annotations: map[string]string{
			"coroot.com/mongodb-scrape": "true",
			"coroot.com/mongodb-scrape-param-sslmode": "verify-full",
			"coroot.com/mongodb-scrape-credentials-secret-name": "creds",
			"coroot.com/mongodb-scrape-tls-secret-name": "tls-secret",
			"coroot.com/mongodb-scrape-tls-secret-ca-key": "ca.crt",
		},
	}

	target := TargetFromPod(pod)
	require.NotNil(t, target)
	assert.Equal(t, "verify-full", target.Params["sslmode"])
	assert.Equal(t, "tls-secret", target.TlsSecret.Name)
	assert.Equal(t, "ca.crt", target.TlsSecret.CaKey)
}

func TestTargetFromPod_MongoDB_VerifyFull_MTLS(t *testing.T) {
	pod := &k8s.Pod{
		Id: k8s.PodId{
			Namespace: "test-ns",
			Name:      "test-pod",
		},
		IP: "10.0.0.1",
		Annotations: map[string]string{
			"coroot.com/mongodb-scrape": "true",
			"coroot.com/mongodb-scrape-param-sslmode": "verify-full",
			"coroot.com/mongodb-scrape-credentials-secret-name": "creds",
			"coroot.com/mongodb-scrape-tls-secret-name": "tls-secret",
			"coroot.com/mongodb-scrape-tls-secret-ca-key": "ca.crt",
			"coroot.com/mongodb-scrape-tls-secret-cert-key": "client.crt",
			"coroot.com/mongodb-scrape-tls-secret-key-key": "client.key",
		},
	}

	target := TargetFromPod(pod)
	require.NotNil(t, target)
	assert.Equal(t, "verify-full", target.Params["sslmode"])
	assert.Equal(t, "tls-secret", target.TlsSecret.Name)
	assert.Equal(t, "ca.crt", target.TlsSecret.CaKey)
	assert.Equal(t, "client.crt", target.TlsSecret.CertKey)
	assert.Equal(t, "client.key", target.TlsSecret.KeyKey)
}

func TestTargetFromPod_MongoDB_DefaultKeys(t *testing.T) {
	pod := &k8s.Pod{
		Id: k8s.PodId{
			Namespace: "test-ns",
			Name:      "test-pod",
		},
		IP: "10.0.0.1",
		Annotations: map[string]string{
			"coroot.com/mongodb-scrape": "true",
			"coroot.com/mongodb-scrape-param-sslmode": "verify-ca",
			"coroot.com/mongodb-scrape-credentials-secret-name": "creds",
			"coroot.com/mongodb-scrape-tls-secret-name": "tls-secret",
		},
	}

	target := TargetFromPod(pod)
	require.NotNil(t, target)
	assert.Equal(t, "verify-ca", target.Params["sslmode"])
	assert.Equal(t, "tls-secret", target.TlsSecret.Name)
	// Check default values
	assert.Equal(t, "ca.crt", target.TlsSecret.CaKey)
	assert.Equal(t, "tls.crt", target.TlsSecret.CertKey)
	assert.Equal(t, "tls.key", target.TlsSecret.KeyKey)
}

// Test Target equality
func TestTarget_Equal_MongoDB(t *testing.T) {
	t1 := &Target{
		Type: TargetTypeMongodb,
		Addr: "10.0.0.1:27017",
		Credentials: Credentials{
			Username: "user",
			Password: "pass",
		},
		Params: map[string]string{
			"sslmode": "verify-full",
		},
		TlsSecret: TlsSecret{
			Namespace: "test-ns",
			Name:      "tls-secret",
			CaKey:     "ca.crt",
			CertKey:   "tls.crt",
			KeyKey:    "tls.key",
		},
	}

	t2 := &Target{
		Type: TargetTypeMongodb,
		Addr: "10.0.0.1:27017",
		Credentials: Credentials{
			Username: "user",
			Password: "pass",
		},
		Params: map[string]string{
			"sslmode": "verify-full",
		},
		TlsSecret: TlsSecret{
			Namespace: "test-ns",
			Name:      "tls-secret",
			CaKey:     "ca.crt",
			CertKey:   "tls.crt",
			KeyKey:    "tls.key",
		},
	}

	assert.True(t, t1.Equal(t2))
}

func TestTarget_NotEqual_MongoDB_DifferentSSLMode(t *testing.T) {
	t1 := &Target{
		Type: TargetTypeMongodb,
		Addr: "10.0.0.1:27017",
		Params: map[string]string{
			"sslmode": "verify-ca",
		},
	}

	t2 := &Target{
		Type: TargetTypeMongodb,
		Addr: "10.0.0.1:27017",
		Params: map[string]string{
			"sslmode": "require",
		},
	}

	assert.False(t, t1.Equal(t2))
}

func TestTarget_NotEqual_MongoDB_DifferentTLSSecret(t *testing.T) {
	t1 := &Target{
		Type: TargetTypeMongodb,
		Addr: "10.0.0.1:27017",
		Params: map[string]string{
			"sslmode": "verify-ca",
		},
		TlsSecret: TlsSecret{
			Name: "secret1",
		},
	}

	t2 := &Target{
		Type: TargetTypeMongodb,
		Addr: "10.0.0.1:27017",
		Params: map[string]string{
			"sslmode": "verify-ca",
		},
		TlsSecret: TlsSecret{
			Name: "secret2",
		},
	}

	assert.False(t, t1.Equal(t2))
}

func TestTarget_Equal_Postgres(t *testing.T) {
	t1 := &Target{
		Type: TargetTypePostgres,
		Addr: "10.0.0.1:5432",
		Params: map[string]string{
			"sslmode": "verify-full",
		},
	}

	t2 := &Target{
		Type: TargetTypePostgres,
		Addr: "10.0.0.1:5432",
		Params: map[string]string{
			"sslmode": "verify-full",
		},
	}

	assert.True(t, t1.Equal(t2))
}

// Test edge cases
func TestTargetFromPod_NotDatabase(t *testing.T) {
	pod := &k8s.Pod{
		Id: k8s.PodId{
			Namespace: "test-ns",
			Name:      "test-pod",
		},
		IP: "10.0.0.1",
		Annotations: map[string]string{
			"some-other-annotation": "value",
		},
	}

	target := TargetFromPod(pod)
	assert.Nil(t, target)
}

func TestTargetFromPod_Nil(t *testing.T) {
	target := TargetFromPod(nil)
	assert.Nil(t, target)
}

func TestTargetFromPod_NilAnnotations(t *testing.T) {
	pod := &k8s.Pod{
		Id: k8s.PodId{
			Namespace: "test-ns",
			Name:      "test-pod",
		},
		IP: "10.0.0.1",
	}

	target := TargetFromPod(pod)
	assert.Nil(t, target)
}

// Test MySQL
func TestTargetFromPod_MySQL(t *testing.T) {
	pod := &k8s.Pod{
		Id: k8s.PodId{
			Namespace: "test-ns",
			Name:      "test-pod",
		},
		IP: "10.0.0.1",
		Annotations: map[string]string{
			"coroot.com/mysql-scrape":                      "true",
			"coroot.com/mysql-scrape-param-tls":            "true",
			"coroot.com/mysql-scrape-credentials-username": "user",
			"coroot.com/mysql-scrape-credentials-password": "pass",
		},
	}

	target := TargetFromPod(pod)
	require.NotNil(t, target)
	assert.Equal(t, TargetTypeMysql, target.Type)
	assert.Equal(t, "true", target.Params["tls"])
}

// Test verify-system mode
func TestTargetFromPod_MongoDB_VerifySystem(t *testing.T) {
	pod := &k8s.Pod{
		Id: k8s.PodId{
			Namespace: "test-ns",
			Name:      "test-pod",
		},
		IP: "10.0.0.1",
		Annotations: map[string]string{
			"coroot.com/mongodb-scrape":                    "true",
			"coroot.com/mongodb-scrape-param-sslmode":      "verify-system",
			"coroot.com/mongodb-scrape-credentials-secret-name": "creds",
		},
	}

	target := TargetFromPod(pod)
	require.NotNil(t, target)
	assert.Equal(t, "verify-system", target.Params["sslmode"])
}

// Helper to generate test CA certificate
func generateTestCA(t *testing.T) []byte {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	template := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Test CA"},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	require.NoError(t, err)

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
}

// generateTestCAWithKey generates test CA certificate and returns both cert PEM and private key
func generateTestCAWithKey(t *testing.T) (caCert []byte, caKey *rsa.PrivateKey) {
	t.Helper()
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	template := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Test CA"},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &caKey.PublicKey, caKey)
	require.NoError(t, err)

	caCert = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	return
}

// generateTestClientCertFromCA generates a client certificate signed by the given CA
func generateTestClientCertFromCA(t *testing.T, caCert []byte, caKey *rsa.PrivateKey) (cert, key []byte) {
	t.Helper()

	// Parse CA cert
	block, _ := pem.Decode(caCert)
	require.NotNil(t, block)
	ca, err := x509.ParseCertificate(block.Bytes)
	require.NoError(t, err)

	// Generate client key
	clientPriv, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	template := x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "Test Client"},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, ca, &clientPriv.PublicKey, caKey)
	require.NoError(t, err)

	cert = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	key = pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(clientPriv)})
	return
}

// Test loadCAFromSecret helper function
func TestLoadCAFromSecret(t *testing.T) {
	caCert := generateTestCA(t)

	tests := []struct {
		name      string
		tlsData   map[string][]byte
		caKey     string
		wantErr   bool
		errSubstr string
	}{
		{
			name:    "valid CA",
			tlsData: map[string][]byte{"ca.crt": caCert},
			caKey:   "ca.crt",
			wantErr: false,
		},
		{
			name:      "empty tlsData",
			tlsData:   nil,
			caKey:     "ca.crt",
			wantErr:   true,
			errSubstr: "TLS secret is required",
		},
		{
			name:      "missing CA key",
			tlsData:   map[string][]byte{"wrong.crt": caCert},
			caKey:     "ca.crt",
			wantErr:   true,
			errSubstr: "CA certificate not found",
		},
		{
			name:      "empty CA data",
			tlsData:   map[string][]byte{"ca.crt": {}},
			caKey:     "ca.crt",
			wantErr:   true,
			errSubstr: "CA certificate not found",
		},
		{
			name:      "invalid CA PEM",
			tlsData:   map[string][]byte{"ca.crt": []byte("invalid pem data")},
			caKey:     "ca.crt",
			wantErr:   true,
			errSubstr: "failed to parse CA certificate",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool, err := loadCAFromSecret(tt.tlsData, tt.caKey)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errSubstr)
				assert.Nil(t, pool)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, pool)
			}
		})
	}
}

// Test StartExporter MongoDB TLS modes
func TestStartExporter_MongoDB_DisableMode(t *testing.T) {
	reg := prometheus.NewRegistry()
	target := &Target{
		Type:   TargetTypeMongodb,
		Addr:   "localhost:27017",
		Params: map[string]string{"sslmode": "disable"},
		logger: logger.NewKlog("test"),
	}

	// This should not error on TLS setup - MongoDB collector may or may not return error
	// depending on connection behavior, but should not error on sslmode configuration
	err := target.StartExporter(reg, Credentials{}, nil, time.Second, time.Second*2)
	// If there is an error, it should be connection related, not TLS mode
	if err != nil {
		assert.NotContains(t, err.Error(), "sslmode")
	}
}

func TestStartExporter_MongoDB_RequireMode(t *testing.T) {
	reg := prometheus.NewRegistry()
	target := &Target{
		Type:   TargetTypeMongodb,
		Addr:   "localhost:27017",
		Params: map[string]string{"sslmode": "require"},
		logger: logger.NewKlog("test"),
	}

	err := target.StartExporter(reg, Credentials{}, nil, time.Second, time.Second*2)
	// If there is an error, it should be connection related, not TLS mode
	if err != nil {
		assert.NotContains(t, err.Error(), "sslmode")
	}
}

func TestStartExporter_MongoDB_VerifySystemMode(t *testing.T) {
	reg := prometheus.NewRegistry()
	target := &Target{
		Type:   TargetTypeMongodb,
		Addr:   "localhost:27017",
		Params: map[string]string{"sslmode": "verify-system"},
		logger: logger.NewKlog("test"),
	}

	err := target.StartExporter(reg, Credentials{}, nil, time.Second, time.Second*2)
	// If there is an error, it should be connection related, not TLS mode
	if err != nil {
		assert.NotContains(t, err.Error(), "sslmode")
	}
}

func TestStartExporter_MongoDB_VerifyCA_MissingSecret(t *testing.T) {
	reg := prometheus.NewRegistry()
	target := &Target{
		Type:   TargetTypeMongodb,
		Addr:   "localhost:27017",
		Params: map[string]string{"sslmode": "verify-ca"},
		TlsSecret: TlsSecret{
			CaKey: "ca.crt",
		},
		logger: logger.NewKlog("test"),
	}

	err := target.StartExporter(reg, Credentials{}, nil, time.Second, time.Second*2)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TLS secret is required")
}

func TestStartExporter_MongoDB_VerifyCA_MissingCA(t *testing.T) {
	reg := prometheus.NewRegistry()
	target := &Target{
		Type:   TargetTypeMongodb,
		Addr:   "localhost:27017",
		Params: map[string]string{"sslmode": "verify-ca"},
		TlsSecret: TlsSecret{
			CaKey: "ca.crt",
		},
		logger: logger.NewKlog("test"),
	}

	tlsData := map[string][]byte{
		"wrong.crt": []byte("some data"),
	}

	err := target.StartExporter(reg, Credentials{}, tlsData, time.Second, time.Second*2)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "CA certificate not found")
}

func TestStartExporter_MongoDB_VerifyCA_InvalidCA(t *testing.T) {
	reg := prometheus.NewRegistry()
	target := &Target{
		Type:   TargetTypeMongodb,
		Addr:   "localhost:27017",
		Params: map[string]string{"sslmode": "verify-ca"},
		TlsSecret: TlsSecret{
			CaKey: "ca.crt",
		},
		logger: logger.NewKlog("test"),
	}

	tlsData := map[string][]byte{
		"ca.crt": []byte("invalid pem data"),
	}

	err := target.StartExporter(reg, Credentials{}, tlsData, time.Second, time.Second*2)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse CA certificate")
}

func TestStartExporter_MongoDB_VerifyFull_MissingSecret(t *testing.T) {
	reg := prometheus.NewRegistry()
	target := &Target{
		Type:   TargetTypeMongodb,
		Addr:   "localhost:27017",
		Params: map[string]string{"sslmode": "verify-full"},
		TlsSecret: TlsSecret{
			CaKey: "ca.crt",
		},
		logger: logger.NewKlog("test"),
	}

	err := target.StartExporter(reg, Credentials{}, nil, time.Second, time.Second*2)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TLS secret is required")
}

func TestStartExporter_MongoDB_VerifyFull_WithMTLS(t *testing.T) {
	// Generate CA and client certs for mTLS test
	caCert, caKey := generateTestCAWithKey(t)
	clientCert, clientKey := generateTestClientCertFromCA(t, caCert, caKey)

	reg := prometheus.NewRegistry()
	target := &Target{
		Type:   TargetTypeMongodb,
		Addr:   "localhost:27017",
		Params: map[string]string{"sslmode": "verify-full"},
		TlsSecret: TlsSecret{
			CaKey:   "ca.crt",
			CertKey: "tls.crt",
			KeyKey:  "tls.key",
		},
		logger: logger.NewKlog("test"),
	}

	tlsData := map[string][]byte{
		"ca.crt":  caCert,
		"tls.crt": clientCert,
		"tls.key": clientKey,
	}

	// This should successfully configure TLS with mTLS (collector creation should work)
	err := target.StartExporter(reg, Credentials{}, tlsData, time.Second, time.Second*2)
	// If there is an error, it should NOT be a TLS configuration error
	if err != nil {
		assert.NotContains(t, err.Error(), "sslmode")
		assert.NotContains(t, err.Error(), "failed to load client certificate")
		assert.NotContains(t, err.Error(), "CA certificate")
	}
}

func TestStartExporter_MongoDB_VerifyFull_InvalidClientCert(t *testing.T) {
	caCert := generateTestCA(t)

	reg := prometheus.NewRegistry()
	target := &Target{
		Type:   TargetTypeMongodb,
		Addr:   "localhost:27017",
		Params: map[string]string{"sslmode": "verify-full"},
		TlsSecret: TlsSecret{
			CaKey:   "ca.crt",
			CertKey: "tls.crt",
			KeyKey:  "tls.key",
		},
		logger: logger.NewKlog("test"),
	}

	tlsData := map[string][]byte{
		"ca.crt":  caCert,
		"tls.crt": []byte("invalid cert"),
		"tls.key": []byte("invalid key"),
	}

	err := target.StartExporter(reg, Credentials{}, tlsData, time.Second, time.Second*2)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to load client certificate")
}

func TestStartExporter_MongoDB_UnsupportedSSLMode(t *testing.T) {
	reg := prometheus.NewRegistry()
	target := &Target{
		Type:   TargetTypeMongodb,
		Addr:   "localhost:27017",
		Params: map[string]string{"sslmode": "invalid-mode"},
		logger: logger.NewKlog("test"),
	}

	err := target.StartExporter(reg, Credentials{}, nil, time.Second, time.Second*2)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported sslmode 'invalid-mode'")
	assert.Contains(t, err.Error(), "verify-system")
}

