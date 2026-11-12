package observe

import (
	"context"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdk "go.opentelemetry.io/otel/sdk/trace"
	"log/slog"
	"os"
)

var Registry = prometheus.NewRegistry()
var Events = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "pulse_events_total", Help: "Events by bounded processing stage and result"}, []string{"stage", "result"})
var Duration = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "pulse_duration_seconds", Help: "Operation latency", Buckets: prometheus.DefBuckets}, []string{"operation"})
var Gauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "pulse_resources", Help: "Bounded queue, worker and connection measurements"}, []string{"resource"})

func Init(ctx context.Context, role string) func(context.Context) error {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", role))
	Registry.MustRegister(Events, Duration, Gauge, prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	otel.SetTextMapPropagator(propagation.TraceContext{})
	opts := []sdk.TracerProviderOption{sdk.WithResource(resource.NewWithAttributes("", attribute.String("service.name", "pulsestream-"+role))), sdk.WithSampler(sdk.ParentBased(sdk.TraceIDRatioBased(0.05)))}
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" {
		ex, err := otlptracehttp.New(ctx)
		if err == nil {
			opts = append(opts, sdk.WithBatcher(ex, sdk.WithMaxQueueSize(2048)))
		} else {
			slog.Error("trace exporter", "error", err)
		}
	}
	tp := sdk.NewTracerProvider(opts...)
	otel.SetTracerProvider(tp)
	return tp.Shutdown
}
