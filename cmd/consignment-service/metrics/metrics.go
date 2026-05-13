package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type metrics struct {
	sagaDuration *prometheus.HistogramVec
	sagaTotal    *prometheus.CounterVec
}

type Metrics interface {
	ObserveSagaDuration(seconds float64, status string)
	IncSagaTotal(status string)
}

func New() Metrics {
	return &metrics{
		sagaDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "shippy_saga_duration_seconds",
			Help:    "Duration from ConfirmConsignment to terminal state",
			Buckets: prometheus.DefBuckets,
		}, []string{"status"}),
		sagaTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "shippy_saga_total",
			Help: "Total sagas by terminal status",
		}, []string{"status"}),
	}
}

func (m *metrics) ObserveSagaDuration(seconds float64, status string) {
	m.sagaDuration.WithLabelValues(status).Observe(seconds)
}

func (m *metrics) IncSagaTotal(status string) {
	m.sagaTotal.WithLabelValues(status).Inc()
}
