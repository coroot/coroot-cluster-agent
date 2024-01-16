package clickhouse

import (
	"context"
	"crypto/tls"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/chpool"
)

type Config struct {
	Address  string `yaml:"address"`
	Database string `yaml:"database"`
	Auth     struct {
		User     string `yaml:"user"`
		Password string `yaml:"password"`
	} `yaml:"auth"`
	TLS struct {
		Enable     bool `yaml:"enable"`
		SkipVerify bool `yaml:"skip_verify"`
	} `yaml:"tls"`
}

type Client struct {
	pool *chpool.Pool
}

func NewClient(cfg Config, clientName string) (*Client, error) {
	opts := ch.Options{
		Address:          cfg.Address,
		Database:         cfg.Database,
		User:             cfg.Auth.User,
		Password:         cfg.Auth.Password,
		Compression:      ch.CompressionLZ4,
		ClientName:       clientName,
		ReadTimeout:      30 * time.Second,
		DialTimeout:      10 * time.Second,
		HandshakeTimeout: 10 * time.Second,
	}
	if cfg.TLS.Enable {
		opts.TLS = &tls.Config{
			InsecureSkipVerify: cfg.TLS.SkipVerify,
		}
	}
	pool, err := chpool.Dial(context.Background(), chpool.Options{
		ClientOptions: opts,
	})
	if err != nil {
		return nil, err
	}

	err = pool.Ping(context.Background())
	if err != nil {
		return nil, err
	}

	return &Client{pool: pool}, nil
}

func (cl *Client) Do(ctx context.Context, q ch.Query) error {
	return cl.pool.Do(ctx, q)
}
