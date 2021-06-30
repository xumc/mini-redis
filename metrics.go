package main

import "github.com/prometheus/client_golang/prometheus"

var (
	connCounterGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "mini_redis",
		Subsystem: "conn",
		Name:      "tcp_connection_count",
		Help:      "tcp connection count",
	})

	pureSetDuration = prometheus.NewSummary(prometheus.SummaryOpts{
		Namespace: "mini_redis",
		Subsystem: "storage",
		Name:       "pure_set_duration",
		Help:       "pure set duration",
	})
	lockSetDuration = prometheus.NewSummary(prometheus.SummaryOpts{
		Namespace: "mini_redis",
		Subsystem: "storage",
		Name:       "lock_set_duration",
		Help:       "lock set duration",
	})

	recvCmdCount = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "mini_redis",
		Subsystem: "parser",
		Name:      "recv_cmd_count",
		Help:      "recv cmd count",
	})
)

func init() {
	prometheus.MustRegister(connCounterGauge)
	prometheus.MustRegister(pureSetDuration)
	prometheus.MustRegister(lockSetDuration)
	prometheus.MustRegister(recvCmdCount)
}
