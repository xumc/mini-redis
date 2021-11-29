package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"io"
	"os"
	"path/filepath"
	"time"
)

var (
	connCounterMetric = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "mini_redis",
		Subsystem: "conn",
		Name:      "tcp_connection_count",
		Help:      "tcp connection count",
	})

	dbFileSizeMetric = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "mini_redis",
		Subsystem: "storage",
		Name:      "db_file_size",
		Help:      "db file size",
	})

	walFileSizeMetric = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "mini_redis",
		Subsystem: "storage",
		Name:      "wal_file_size",
		Help:      "wal file size",
	})

	walCheckpointMetric = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "mini_redis",
		Subsystem: "storage",
		Name:      "wal_checkpoint_size",
		Help:      "wal checkpoint size",
	})

	pureSetDurationMetric = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "mini_redis",
		Subsystem: "storage",
		Name:       "pure_set_duration",
		Help:       "pure set duration",
	})

	lockSetDurationMetric = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "mini_redis",
		Subsystem: "storage",
		Name:       "lock_set_duration",
		Help:       "lock set duration",
	})

	pureGetDurationMetric = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "mini_redis",
		Subsystem: "storage",
		Name:       "pure_get_duration",
		Help:       "pure get duration",
	})

	lockGetDurationMetric = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "mini_redis",
		Subsystem: "storage",
		Name:       "lock_get_duration",
		Help:       "lock get duration",
	})

	pureDelDurationMetric = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "mini_redis",
		Subsystem: "storage",
		Name:       "pure_del_duration",
		Help:       "pure del duration",
	})

	lockDelDurationMetric = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "mini_redis",
		Subsystem: "storage",
		Name:       "lock_del_duration",
		Help:       "lock del duration",
	})

	recvCmdCountMetric = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "mini_redis",
		Subsystem: "parser",
		Name:      "recv_cmd_count",
		Help:      "recv cmd count",
	})
)

func init() {
	// handler
	prometheus.MustRegister(connCounterMetric)
	prometheus.MustRegister(dbFileSizeMetric)
	prometheus.MustRegister(walFileSizeMetric)
	prometheus.MustRegister(walCheckpointMetric)

	// storage
	prometheus.MustRegister(pureSetDurationMetric)
	prometheus.MustRegister(lockSetDurationMetric)
	prometheus.MustRegister(pureGetDurationMetric)
	prometheus.MustRegister(lockGetDurationMetric)
	prometheus.MustRegister(pureDelDurationMetric)
	prometheus.MustRegister(lockDelDurationMetric)

	// misc
	prometheus.MustRegister(recvCmdCountMetric)


}

func startMonitor() {
	go func() {
		dbPath := filepath.Join("data", "db")
		dbfile, err := os.OpenFile(dbPath, os.O_CREATE|os.O_RDONLY, 0644)
		if err != nil {
			logrus.Errorf("monitor checkpoint error. %s", err.Error())
		}
		defer dbfile.Close()

		tick := time.NewTicker(time.Second)

		for {

			select {
			case <- tick.C:
				bs := make([]byte, os.Getpagesize())
				n, err := dbfile.ReadAt(bs, 0)
				if err != nil {
					if err == io.EOF {
						continue
					}
					logrus.Errorf("monitor checkpoint error. %s", err.Error())
				}

				if n != os.Getpagesize() {
					continue
				}

				cp := pageInBuffer(bs[:n], 0).meta().checkpoint
				walCheckpointMetric.Set(float64(cp))
			}
		}
	}()
}
