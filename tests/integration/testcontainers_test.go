package integration

import (
	"context"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"net"
	"os"
	"testing"
	"time"
)

func TestRedisContainerQuotaTTL(t *testing.T) {
	if os.Getenv("PULSE_TESTCONTAINERS") != "1" {
		t.Skip("set PULSE_TESTCONTAINERS=1 with Docker socket access")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{Image: "redis:8-alpine", ExposedPorts: []string{"6379/tcp"}, WaitingFor: wait.ForLog("Ready to accept connections")}, Started: true})
	if err != nil {
		t.Fatal(err)
	}
	defer container.Terminate(context.Background())
	host, err := container.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := container.MappedPort(ctx, "6379/tcp")
	if err != nil {
		t.Fatal(err)
	}
	cl := redis.NewClient(&redis.Options{Addr: net.JoinHostPort(host, port.Port())})
	defer cl.Close()
	script := redis.NewScript(`local n=redis.call('INCRBY',KEYS[1],ARGV[1]); if n==tonumber(ARGV[1]) then redis.call('EXPIRE',KEYS[1],2) end; return n`)
	for i := int64(1); i <= 5; i++ {
		v, err := script.Run(ctx, cl, []string{"quota:test"}, 100).Int64()
		if err != nil || v != i*100 {
			t.Fatal(v, err)
		}
	}
	ttl, err := cl.TTL(ctx, "quota:test").Result()
	if err != nil || ttl <= 0 || ttl > 2*time.Second {
		t.Fatal("quota TTL", ttl, err)
	}
}
