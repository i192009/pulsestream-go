package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Role, HTTP, GRPC, Database, ClickHouse, ClickHousePassword, Redis, Origin, Group string
	Brokers                                                                          []string
	Keys                                                                             map[string]Identity
	TenantRates                                                                      map[string]int
	Partitions, Rate, MaxBatch, Poll, MaxStateBytes                                  int
	Lateness, OutOfOrder                                                             time.Duration
}
type Identity struct{ Tenant, Role string }

func Env(k, fallback string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return fallback
}
func integer(k string, n int) int {
	v, e := strconv.Atoi(Env(k, strconv.Itoa(n)))
	if e != nil || v < 1 {
		panic("invalid " + k)
	}
	return v
}
func duration(k, s string) time.Duration {
	v, e := time.ParseDuration(Env(k, s))
	if e != nil || v < 0 {
		panic("invalid " + k)
	}
	return v
}
func Load(role string) Config {
	c := Config{Role: role, HTTP: Env("PULSE_HTTP", ":8080"), GRPC: Env("PULSE_GRPC", ":9090"), Database: Env("PULSE_DATABASE_URL", "postgres://pulse:pulse-dev@localhost:5432/pulse?sslmode=disable"), ClickHouse: Env("PULSE_CLICKHOUSE", "http://localhost:8123"), ClickHousePassword: Env("PULSE_CLICKHOUSE_PASSWORD", "pulse-dev"), Redis: Env("PULSE_REDIS", "localhost:6379"), Origin: Env("PULSE_ORIGIN", "http://localhost:13000"), Group: Env("PULSE_GROUP", "pulsestream-v1"), Brokers: strings.Split(Env("PULSE_BROKERS", "localhost:19092"), ","), Keys: map[string]Identity{}, Partitions: integer("PULSE_PARTITIONS", 8), Rate: integer("PULSE_RATE", 10000), MaxBatch: integer("PULSE_MAX_BATCH", 500), Poll: integer("PULSE_POLL", 1000), MaxStateBytes: integer("PULSE_MAX_STATE_BYTES", 32<<20), Lateness: duration("PULSE_LATENESS", "2m"), OutOfOrder: duration("PULSE_OUT_OF_ORDER", "10s")}
	for _, key := range strings.Split(Env("PULSE_KEYS", ""), ",") {
		p := strings.Split(key, ":")
		if len(p) != 3 || p[0] == "" || p[1] == "" || (p[2] != "admin" && p[2] != "reader" && p[2] != "writer") {
			panic("PULSE_KEYS must contain key:tenant:role entries")
		}
		c.Keys[p[0]] = Identity{Tenant: p[1], Role: p[2]}
	}
	c.TenantRates = map[string]int{}
	for _, entry := range strings.Split(Env("PULSE_TENANT_RATES", ""), ",") {
		if entry == "" {
			continue
		}
		parts := strings.Split(entry, ":")
		if len(parts) != 2 {
			panic("invalid PULSE_TENANT_RATES")
		}
		n, err := strconv.Atoi(parts[1])
		if err != nil || n < 1 {
			panic("invalid tenant rate")
		}
		c.TenantRates[parts[0]] = n
	}
	return c
}
