# Coroot Cluster Agent

Coroot Cluster Agent collects metrics from various databases and services running in Kubernetes clusters.

## Supported Databases

- PostgreSQL
- MySQL
- Redis
- MongoDB
- Memcached

## TLS/SSL Support

All databases use a unified approach for TLS configuration using the `sslmode` parameter (PostgreSQL, MongoDB) or `tls` parameter (MySQL).

### SSL Modes

| Mode | Description | Security Level | Use Case | Supported |
|------|-------------|----------------|----------|-----------|
| **disable** | No encryption | ⚠️ None | Development only | PostgreSQL, MongoDB |
| **require** | Encrypted connection without certificate verification | ⚠️ Low | Testing with self-signed certs | PostgreSQL, MongoDB |
| **verify-system** | Encrypted + system CA verification | ✅ Medium | Production with public CA (Let's Encrypt, etc.) | MongoDB only |
| **verify-ca** | Encrypted + custom CA verification from Secret | ✅ Medium | Production with private CA | PostgreSQL, MongoDB |
| **verify-full** | Encrypted + CA + hostname verification | ✅ High | Production (recommended) | PostgreSQL, MongoDB |

## PostgreSQL Configuration

### Basic Configuration (No TLS)

```yaml
apiVersion: v1
kind: Pod
metadata:
  annotations:
    coroot.com/postgres-scrape: "true"
    coroot.com/postgres-scrape-port: "5432"
    coroot.com/postgres-scrape-credentials-secret-name: "postgres-credentials"
    coroot.com/postgres-scrape-credentials-secret-username-key: "username"
    coroot.com/postgres-scrape-credentials-secret-password-key: "password"
```

### With TLS (verify-full mode)

```yaml
apiVersion: v1
kind: Pod
metadata:
  annotations:
    coroot.com/postgres-scrape: "true"
    coroot.com/postgres-scrape-port: "5432"
    coroot.com/postgres-scrape-param-sslmode: "verify-full"
    coroot.com/postgres-scrape-credentials-secret-name: "postgres-credentials"
    coroot.com/postgres-scrape-credentials-secret-username-key: "username"
    coroot.com/postgres-scrape-credentials-secret-password-key: "password"
```

### Available SSL Modes for PostgreSQL

- `disable` - No SSL (default)
- `require` - SSL without certificate verification
- `verify-ca` - SSL with CA certificate verification
- `verify-full` - SSL with full verification (CA + hostname)

**Note:** PostgreSQL uses the system CA pool or certificates from the PostgreSQL driver's default locations.

## MongoDB Configuration

### Basic Configuration (No TLS)

```yaml
apiVersion: v1
kind: Pod
metadata:
  annotations:
    coroot.com/mongodb-scrape: "true"
    coroot.com/mongodb-scrape-port: "27017"
    coroot.com/mongodb-scrape-credentials-secret-name: "mongodb-credentials"
    coroot.com/mongodb-scrape-credentials-secret-username-key: "username"
    coroot.com/mongodb-scrape-credentials-secret-password-key: "password"
```

### TLS with Custom CA

```yaml
apiVersion: v1
kind: Pod
metadata:
  annotations:
    coroot.com/mongodb-scrape: "true"
    coroot.com/mongodb-scrape-port: "27017"
    coroot.com/mongodb-scrape-param-sslmode: "verify-ca"
    coroot.com/mongodb-scrape-credentials-secret-name: "mongodb-credentials"
    coroot.com/mongodb-scrape-credentials-secret-username-key: "username"
    coroot.com/mongodb-scrape-credentials-secret-password-key: "password"
    coroot.com/mongodb-scrape-tls-secret-name: "mongodb-tls"
    coroot.com/mongodb-scrape-tls-secret-ca-key: "ca.crt"
```

### TLS with Mutual Authentication (mTLS)

```yaml
apiVersion: v1
kind: Pod
metadata:
  annotations:
    coroot.com/mongodb-scrape: "true"
    coroot.com/mongodb-scrape-port: "27017"
    coroot.com/mongodb-scrape-param-sslmode: "verify-full"
    coroot.com/mongodb-scrape-credentials-secret-name: "mongodb-credentials"
    coroot.com/mongodb-scrape-credentials-secret-username-key: "username"
    coroot.com/mongodb-scrape-credentials-secret-password-key: "password"
    coroot.com/mongodb-scrape-tls-secret-name: "mongodb-tls"
    coroot.com/mongodb-scrape-tls-secret-ca-key: "ca.crt"
    coroot.com/mongodb-scrape-tls-secret-cert-key: "tls.crt"
    coroot.com/mongodb-scrape-tls-secret-key-key: "tls.key"
```

### Available SSL Modes for MongoDB

- `disable` - No TLS (default)
- `require` - TLS without certificate verification (⚠️ insecure)
- `verify-system` - TLS with system CA pool verification (for public CAs like Let's Encrypt). **Note:** This mode is specific to MongoDB and not available for PostgreSQL.
- `verify-ca` - TLS with custom CA certificate verification from Kubernetes Secret
- `verify-full` - TLS with CA verification + hostname verification + optional client certificate (mTLS)

> **Important:** When using `verify-full` mode, hostname verification requires the server certificate to contain the correct hostname in the Subject Alternative Name (SAN) extension. If connecting via IP address, the certificate must include that IP in its SAN. Using DNS names is recommended for `verify-full` mode.

### Kubernetes Secret Examples

#### Credentials Secret

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: mongodb-credentials
  namespace: default
type: Opaque
stringData:
  username: "monitor"
  password: "secret-password"
```

#### TLS Secret with CA Only

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: mongodb-tls
  namespace: default
type: Opaque
data:
  ca.crt: LS0tLS1CRUdJTi... # Base64-encoded CA certificate
```

#### TLS Secret with mTLS

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: mongodb-tls
  namespace: default
type: kubernetes.io/tls
data:
  ca.crt: LS0tLS1CRUdJTi... # Base64-encoded CA certificate
  tls.crt: LS0tLS1CRUdJTi... # Base64-encoded client certificate
  tls.key: LS0tLS1CRUdJTi... # Base64-encoded client private key
```

## MySQL Configuration

### Basic Configuration

```yaml
apiVersion: v1
kind: Pod
metadata:
  annotations:
    coroot.com/mysql-scrape: "true"
    coroot.com/mysql-scrape-port: "3306"
    coroot.com/mysql-scrape-credentials-secret-name: "mysql-credentials"
    coroot.com/mysql-scrape-credentials-secret-username-key: "username"
    coroot.com/mysql-scrape-credentials-secret-password-key: "password"
```

### With TLS

```yaml
apiVersion: v1
kind: Pod
metadata:
  annotations:
    coroot.com/mysql-scrape: "true"
    coroot.com/mysql-scrape-port: "3306"
    coroot.com/mysql-scrape-param-tls: "true"
    coroot.com/mysql-scrape-credentials-secret-name: "mysql-credentials"
    coroot.com/mysql-scrape-credentials-secret-username-key: "username"
    coroot.com/mysql-scrape-credentials-secret-password-key: "password"
```

### Available TLS Options for MySQL

- `false` - No TLS (default)
- `true` - TLS enabled
- `skip-verify` - TLS without certificate verification
- `preferred` - Use TLS if available

## Redis Configuration

```yaml
apiVersion: v1
kind: Pod
metadata:
  annotations:
    coroot.com/redis-scrape: "true"
    coroot.com/redis-scrape-port: "6379"
    coroot.com/redis-scrape-credentials-secret-name: "redis-credentials"
    coroot.com/redis-scrape-credentials-secret-username-key: "username"
    coroot.com/redis-scrape-credentials-secret-password-key: "password"
```

## Memcached Configuration

```yaml
apiVersion: v1
kind: Pod
metadata:
  annotations:
    coroot.com/memcached-scrape: "true"
    coroot.com/memcached-scrape-port: "11211"
```

## Annotation Reference

### PostgreSQL Annotations

| Annotation | Default | Description |
|------------|---------|-------------|
| `coroot.com/postgres-scrape` | - | Enable scraping (set to `"true"`) |
| `coroot.com/postgres-scrape-port` | `5432` | PostgreSQL port |
| `coroot.com/postgres-scrape-param-sslmode` | `disable` | SSL mode: `disable`, `require`, `verify-ca`, `verify-full` |
| `coroot.com/postgres-scrape-credentials-username` | - | Direct username (not recommended) |
| `coroot.com/postgres-scrape-credentials-password` | - | Direct password (not recommended) |
| `coroot.com/postgres-scrape-credentials-secret-name` | - | Secret name for credentials |
| `coroot.com/postgres-scrape-credentials-secret-username-key` | - | Key for username in secret |
| `coroot.com/postgres-scrape-credentials-secret-password-key` | - | Key for password in secret |

### MongoDB Annotations

| Annotation | Default | Description |
|------------|---------|-------------|
| `coroot.com/mongodb-scrape` | - | Enable scraping (set to `"true"`) |
| `coroot.com/mongodb-scrape-port` | `27017` | MongoDB port |
| `coroot.com/mongodb-scrape-param-sslmode` | `disable` | SSL mode: `disable`, `require`, `verify-system`, `verify-ca`, `verify-full` |
| `coroot.com/mongodb-scrape-credentials-username` | - | Direct username (not recommended) |
| `coroot.com/mongodb-scrape-credentials-password` | - | Direct password (not recommended) |
| `coroot.com/mongodb-scrape-credentials-secret-name` | - | Secret name for credentials |
| `coroot.com/mongodb-scrape-credentials-secret-username-key` | - | Key for username in secret |
| `coroot.com/mongodb-scrape-credentials-secret-password-key` | - | Key for password in secret |
| `coroot.com/mongodb-scrape-tls-secret-name` | - | Secret name for TLS certificates (for `verify-ca` and `verify-full` modes) |
| `coroot.com/mongodb-scrape-tls-secret-ca-key` | `ca.crt` | Key for CA certificate in TLS secret |
| `coroot.com/mongodb-scrape-tls-secret-cert-key` | `tls.crt` | Key for client certificate in TLS secret (for mTLS) |
| `coroot.com/mongodb-scrape-tls-secret-key-key` | `tls.key` | Key for client private key in TLS secret (for mTLS) |

### MySQL Annotations

| Annotation | Default | Description |
|------------|---------|-------------|
| `coroot.com/mysql-scrape` | - | Enable scraping (set to `"true"`) |
| `coroot.com/mysql-scrape-port` | `3306` | MySQL port |
| `coroot.com/mysql-scrape-param-tls` | `false` | TLS mode: `false`, `true`, `skip-verify`, `preferred` |
| `coroot.com/mysql-scrape-credentials-username` | - | Direct username (not recommended) |
| `coroot.com/mysql-scrape-credentials-password` | - | Direct password (not recommended) |
| `coroot.com/mysql-scrape-credentials-secret-name` | - | Secret name for credentials |
| `coroot.com/mysql-scrape-credentials-secret-username-key` | - | Key for username in secret |
| `coroot.com/mysql-scrape-credentials-secret-password-key` | - | Key for password in secret |

### Redis Annotations

| Annotation | Default | Description |
|------------|---------|-------------|
| `coroot.com/redis-scrape` | - | Enable scraping (set to `"true"`) |
| `coroot.com/redis-scrape-port` | `6379` | Redis port |
| `coroot.com/redis-scrape-credentials-username` | - | Direct username (not recommended) |
| `coroot.com/redis-scrape-credentials-password` | - | Direct password (not recommended) |
| `coroot.com/redis-scrape-credentials-secret-name` | - | Secret name for credentials |
| `coroot.com/redis-scrape-credentials-secret-username-key` | - | Key for username in secret |
| `coroot.com/redis-scrape-credentials-secret-password-key` | - | Key for password in secret |

### Memcached Annotations

| Annotation | Default | Description |
|------------|---------|-------------|
| `coroot.com/memcached-scrape` | - | Enable scraping (set to `"true"`) |
| `coroot.com/memcached-scrape-port` | `11211` | Memcached port |

## RBAC Requirements

The Coroot Cluster Agent requires permissions to read Kubernetes Secrets when using credential or TLS secret annotations:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: coroot-cluster-agent
rules:
- apiGroups: [""]
  resources: ["secrets"]
  verbs: ["get", "list"]
- apiGroups: [""]
  resources: ["pods", "events"]
  verbs: ["get", "list", "watch"]
```

## Security Best Practices

1. **Always use Secrets for credentials** instead of annotations
2. **Use `verify-full` mode in production** for PostgreSQL and MongoDB
3. **Avoid `require` mode** (no certificate verification) in production
4. **Rotate credentials regularly** using Kubernetes Secrets
5. **Use mTLS** when the database requires client certificates
6. **Monitor TLS certificate expiration** dates

## License

See [LICENSE](LICENSE) file.
