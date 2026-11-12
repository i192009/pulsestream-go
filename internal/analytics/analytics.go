package analytics

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/pulsestream/pulsestream-go/internal/sink"
	"github.com/pulsestream/pulsestream-go/internal/windows"
	"math"
	"sort"
	"strconv"
	"time"
)

type Window struct {
	*windows.Aggregate
	ActiveUsers     int                `json:"active_users"`
	EPS             float64            `json:"eps"`
	FailureRate     float64            `json:"failure_rate"`
	ErrorRate       float64            `json:"error_rate"`
	Conversion      float64            `json:"conversion"`
	AverageOrder    map[string]float64 `json:"average_order_minor"`
	RevenueExact    map[string]string  `json:"revenue_minor_exact"`
	AvgLatency      float64            `json:"avg_latency_ms"`
	P95Latency      int                `json:"p95_latency_ms"`
	AvgLag          float64            `json:"avg_processing_lag_ms"`
	P95Lag          int                `json:"p95_processing_lag_ms"`
	Shards          int                `json:"shards"`
	MaxAccountCount int64              `json:"max_account_count"`
}

func Query(ctx context.Context, ch *sink.ClickHouse, tenant, pipeline string, from, to int64, size, step int64) ([]Window, error) {
	params := map[string]string{"tenant": tenant, "pipeline": pipeline, "from": strconv.FormatInt(from, 10), "to": strconv.FormatInt(to, 10), "size": strconv.FormatInt(size, 10), "step": strconv.FormatInt(step, 10)}
	b, err := ch.Do(ctx, `SELECT data FROM windows FINAL WHERE tenant={tenant:String} AND pipeline={pipeline:String} AND start >= {from:Int64} AND start < {to:Int64} AND size={size:Int64} AND step={step:Int64} ORDER BY start FORMAT JSONEachRow`, params, nil)
	if err != nil {
		return nil, err
	}
	byStart := map[int64]*Window{}
	scanner := bufio.NewScanner(bytes.NewReader(b))
	scanner.Buffer(make([]byte, 4096), 4<<20)
	for scanner.Scan() {
		var row struct {
			Data string `json:"data"`
		}
		if err = json.Unmarshal(scanner.Bytes(), &row); err != nil {
			return nil, err
		}
		var a windows.Aggregate
		if err = json.Unmarshal([]byte(row.Data), &a); err != nil {
			return nil, err
		}
		w := byStart[a.Start]
		if w == nil {
			copy := a
			w = &Window{Aggregate: &copy, Shards: 1}
			byStart[a.Start] = w
			continue
		}
		w.Shards++
		if err = Merge(w.Aggregate, &a); err != nil {
			return nil, err
		}
	}
	if err = scanner.Err(); err != nil {
		return nil, err
	}
	out := make([]Window, 0, len(byStart))
	for _, w := range byStart {
		w.ActiveUsers = len(w.Users)
		w.EPS = float64(w.Count) / (float64(w.Size) / 1000)
		w.FailureRate = ratio(w.Failed, w.Success+w.Failed)
		w.ErrorRate = ratio(w.Errors, w.Count)
		w.Conversion = ratio(w.Orders, w.Checkouts)
		w.AvgLatency = ratio(w.LatencySum, w.LatencyCount)
		w.P95Latency = windows.Quantile(w.Latencies, .95)
		w.AvgLag = ratio(w.LagSum, w.Count)
		w.P95Lag = windows.Quantile(w.LagHistogram, .95)
		w.AverageOrder = map[string]float64{}
		w.RevenueExact = map[string]string{}
		for currency, minor := range w.Revenue {
			w.RevenueExact[currency] = strconv.FormatInt(minor, 10)
			w.AverageOrder[currency] = float64(minor) / float64(max(w.Payments[currency], 1))
		}
		for _, n := range w.Accounts {
			if n > w.MaxAccountCount {
				w.MaxAccountCount = n
			}
		}
		w.Accounts = nil
		w.Users = nil
		w.Latencies = nil
		w.LagHistogram = nil
		out = append(out, *w)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Start < out[j].Start })
	return out, nil
}
func ratio(a, b int64) float64 {
	if b == 0 {
		return 0
	}
	return float64(a) / float64(b)
}
func Merge(a, b *windows.Aggregate) error {
	for currency, minor := range b.Revenue {
		if minor < 0 || a.Revenue[currency] > math.MaxInt64-minor {
			return fmt.Errorf("revenue total exceeds int64 capacity")
		}
	}
	a.Count += b.Count
	a.Success += b.Success
	a.Failed += b.Failed
	a.Checkouts += b.Checkouts
	a.Orders += b.Orders
	a.Errors += b.Errors
	a.LatencySum += b.LatencySum
	a.LatencyCount += b.LatencyCount
	a.LagSum += b.LagSum
	a.Closed = a.Closed && b.Closed
	a.Final = a.Final && b.Final
	if b.Revision > a.Revision {
		a.Revision = b.Revision
	}
	if a.Payments == nil {
		a.Payments = map[string]int64{}
	}
	for k, v := range b.Payments {
		a.Payments[k] += v
	}
	for k, v := range b.Revenue {
		a.Revenue[k] += v
	}
	for k := range b.Users {
		a.Users[k] = true
	}
	for k, v := range b.Accounts {
		a.Accounts[k] += v
	}
	for k, v := range b.Types {
		a.Types[k] += v
	}
	for k, v := range b.Services {
		x := a.Services[k]
		x[0] += v[0]
		x[1] += v[1]
		a.Services[k] = x
	}
	for k, v := range b.Latencies {
		a.Latencies[k] += v
	}
	for k, v := range b.LagHistogram {
		a.LagHistogram[k] += v
	}

	return nil
}
func Events(ctx context.Context, ch *sink.ClickHouse, tenant, pipeline, kind, search string, from, to int64, limit, offset int) (json.RawMessage, error) {
	p := map[string]string{"tenant": tenant, "pipeline": pipeline, "kind": kind, "search": search, "from": fmt.Sprint(from), "to": fmt.Sprint(to), "limit": fmt.Sprint(limit), "offset": fmt.Sprint(offset)}
	b, err := ch.Do(ctx, `SELECT tenant,event_id,event_type,event_time,partition_id,offset_id,late,data FROM events FINAL WHERE tenant={tenant:String} AND pipeline={pipeline:String} AND event_time >= {from:Int64} AND event_time < {to:Int64} AND ({kind:String}='' OR event_type={kind:String}) AND ({search:String}='' OR positionCaseInsensitive(data,{search:String})>0) ORDER BY event_time DESC LIMIT {limit:UInt32} OFFSET {offset:UInt32} FORMAT JSON`, p, nil)
	if err != nil {
		return nil, err
	}
	return b, nil
}
func Range(hours int) (int64, int64) {
	now := time.Now()
	return now.Add(-time.Duration(hours) * time.Hour).UnixMilli(), now.Add(time.Minute).UnixMilli()
}
