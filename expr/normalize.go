package expr

import (
	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/consolidation"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/util"
)

// normalize normalizes series to the same common LCM interval - if they don't already have the same interval
func normalize(in []models.Series) []models.Series {
	var intervals []uint32
	for _, s := range in {
		intervals = append(intervals, s.Interval)
	}
	lcm := util.Lcm(intervals)
	for i, s := range in {
		if s.Interval != lcm {
			// we need to copy the datapoints first because the consolidater will reuse the input slice
			datapoints := pointSlicePool.Get().([]schema.Point)
			datapoints = append(datapoints, s.Datapoints...)
			consolidation.Consolidate(datapoints, lcm/s.Interval, s.Consolidator) // TODO: not sure if we should use s.Consolidator here
			in[i].Datapoints = datapoints
		}
	}
	return in
}
