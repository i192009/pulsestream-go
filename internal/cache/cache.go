package cache

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"time"
)

type Cache struct{ Client *redis.Client }

func New(addr string) *Cache {
	return &Cache{redis.NewClient(&redis.Options{Addr: addr, PoolSize: 16, MinIdleConns: 1, ReadTimeout: time.Second, WriteTimeout: time.Second, DialTimeout: time.Second, MaxRetries: 1})}
}

var quota = redis.NewScript(`local n=redis.call('INCRBY',KEYS[1],ARGV[1]); if n==tonumber(ARGV[1]) then redis.call('EXPIRE',KEYS[1],2) end; return n`)

func (c *Cache) Allow(ctx context.Context, tenant string, n, rate int) (bool, error) {
	v, err := quota.Run(ctx, c.Client, []string{fmt.Sprintf("quota:%s:%d", tenant, time.Now().Unix())}, n).Int64()
	return v <= int64(rate), err
}
func (c *Cache) Get(ctx context.Context, key string) []byte {
	v, err := c.Client.Get(ctx, key).Bytes()
	if err != nil {
		return nil
	}
	return v
}
func (c *Cache) Put(ctx context.Context, key string, v []byte) {
	_ = c.Client.Set(ctx, key, v, 2*time.Second).Err()
}
