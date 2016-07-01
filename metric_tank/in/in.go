package in

import (
	"fmt"
	"time"

	"github.com/grafana/grafana/pkg/log"
	"github.com/raintank/met"
	"github.com/raintank/raintank-metric/metric_tank/defcache"
	"github.com/raintank/raintank-metric/metric_tank/mdata"
	"github.com/raintank/raintank-metric/metric_tank/usage"
	"github.com/raintank/raintank-metric/msg"
	"github.com/raintank/raintank-metric/schema"
)

type In struct {
	metricsPerMessage met.Meter
	metricsReceived   met.Count
	msgsAge           met.Meter // in ms
	tmp               msg.MetricData

	metrics  mdata.Metrics
	defCache *defcache.DefCache
	usage    *usage.Usage
}

func New(metrics mdata.Metrics, defCache *defcache.DefCache, usage *usage.Usage, input string, stats met.Backend) In {
	return In{
		metricsPerMessage: stats.NewMeter(fmt.Sprintf("%s.metrics_per_message", input), 0),
		metricsReceived:   stats.NewCount(fmt.Sprintf("%s.metrics_received", input)),
		msgsAge:           stats.NewMeter(fmt.Sprintf("%s.message_age", input), 0),
		tmp:               msg.MetricData{Metrics: make([]*schema.MetricData, 1)},

		metrics:  metrics,
		defCache: defCache,
		usage:    usage,
	}
}

func (in In) Handle(data []byte) {
	err := in.tmp.InitFromMsg(data)
	if err != nil {
		log.Error(3, "skipping message. %s", err)
		return
	}
	in.msgsAge.Value(time.Now().Sub(in.tmp.Produced).Nanoseconds() / 1000)

	err = in.tmp.DecodeMetricData() // reads metrics from in.tmp.Msg and unsets it
	if err != nil {
		log.Error(3, "skipping message. %s", err)
		return
	}
	in.metricsPerMessage.Value(int64(len(in.tmp.Metrics)))
	in.metricsReceived.Inc(int64(len(in.tmp.Metrics)))

	for _, metric := range in.tmp.Metrics {
		if metric == nil {
			continue
		}
		if metric.Id == "" {
			log.Error(3, "empty metric.Id - fix your datastream")
			continue
		}
		if metric.Time == 0 {
			log.Warn("invalid metric. metric.Time is 0. %s", metric.Id)
		} else {
			in.defCache.Add(metric)
			m := in.metrics.GetOrCreate(metric.Id)
			m.Add(uint32(metric.Time), metric.Value)
			if in.usage != nil {
				in.usage.Add(metric.OrgId, metric.Id)
			}
		}
	}
}
