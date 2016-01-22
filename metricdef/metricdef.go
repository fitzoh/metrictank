/*
 * Copyright (c) 2015, Raintank Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package metricdef

import (
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/grafana/grafana/pkg/log"
	elastigo "github.com/mattbaird/elastigo/lib"
	"github.com/raintank/raintank-metric/schema"
)

var es *elastigo.Conn
var Indexer *elastigo.BulkIndexer
var IndexName = "metric"

// for the first 30minutes after startup, only
// write to ES 1% of the time. This allows us to
// slowly warmup a new or stale index.
var warmUpDuration = 1800
var warmUpPercent = 1
var startTime time.Time

func InitElasticsearch(addr, user, pass, indexName string, warmupPct int) error {
	IndexName = indexName
	warmUpPercent = warmupPct
	startTime = time.Now()
	rand.Seed(startTime.Unix())

	es = elastigo.NewConn()
	parts := strings.Split(addr, ":")
	if len(parts) != 2 {
		return fmt.Errorf("invalid tcp addr %q", addr)
	}
	es.Domain = parts[0]
	es.Port = parts[1]
	if user != "" && pass != "" {
		es.Username = user
		es.Password = pass
	}
	if exists, err := es.ExistsIndex(IndexName, "", nil); err != nil && err.Error() != "record not found" {
		return err
	} else {
		if !exists {
			log.Info("initializing %s Index with mapping", IndexName)
			//lets apply the mapping.
			metricMapping := `{
				"mappings": {
		            "_default_": {
		                "dynamic_templates": [
		                    {
		                        "strings": {
		                            "mapping": {
		                                "index": "not_analyzed",
		                                "type": "string"
		                            },
		                            "match_mapping_type": "string"
		                        }
		                    }
		                ],
		                "_all": {
		                    "enabled": false
		                },
		                "properties": {}
		            },
		            "metric_index": {
		                "dynamic_templates": [
		                    {
		                        "strings": {
		                            "mapping": {
		                                "index": "not_analyzed",
		                                "type": "string"
		                            },
		                            "match_mapping_type": "string"
		                        }
		                    }
		                ],
		                "_all": {
		                    "enabled": false
		                },
		                "_timestamp": {
		                    "enabled": false
		                },
		                "properties": {
		                    "id": {
		                        "type": "string",
		                        "index": "not_analyzed"
		                    },
		                    "interval": {
		                        "type": "long"
		                    },
		                    "lastUpdate": {
		                        "type": "long"
		                    },
		                    "metric": {
		                        "type": "string",
		                        "index": "not_analyzed"
		                    },
		                    "name": {
		                        "type": "string",
		                        "index": "not_analyzed"
		                    },
		                    "node_count": {
		                        "type": "long"
		                    },
		                    "org_id": {
		                        "type": "long"
		                    },
		                    "tags": {
		                        "type": "string",
		                        "index": "not_analyzed"
		                    },
		                    "target_type": {
		                        "type": "string",
		                        "index": "not_analyzed"
		                    },
		                    "unit": {
		                        "type": "string",
		                        "index": "not_analyzed"
		                    }
		                }
					}
				}
			}`

			_, err = es.DoCommand("PUT", fmt.Sprintf("/%s", IndexName), nil, metricMapping)
			if err != nil {
				return err
			}
		}
	}

	//TODO:(awoods) make the following tuneable
	Indexer = es.NewBulkIndexer(20)
	//dont retry sends.
	Indexer.RetryForSeconds = 0
	// index at most 10k docs per request.
	Indexer.BulkMaxDocs = 10000
	//flush at least every 10seconds.
	Indexer.BufferDelayMax = time.Second * 10
	Indexer.Refresh = true

	Indexer.Start()
	return nil
}

// if scroll_id specified, will resume that scroll session.
// returns scroll_id if there's any more metrics to be fetched.
func GetMetrics(scroll_id string) ([]*schema.MetricDefinition, string, error) {
	// future optimiz: clear scroll when finished, tweak length of items, order by _doc
	// see https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-scroll.html
	defs := make([]*schema.MetricDefinition, 0)
	var err error
	var out elastigo.SearchResult
	if scroll_id == "" {
		out, err = es.Search(IndexName, "metric_index", map[string]interface{}{"scroll": "1m"}, nil)
	} else {
		out, err = es.Scroll(map[string]interface{}{"scroll": "1m"}, scroll_id)
	}
	if err != nil {
		return defs, "", err
	}
	for _, h := range out.Hits.Hits {
		mdef, err := schema.MetricDefinitionFromJSON(*h.Source)
		if err != nil {
			return defs, "", err
		}
		defs = append(defs, mdef)
	}
	scroll_id = ""
	if out.Hits.Len() > 0 {
		scroll_id = out.ScrollId
	}

	return defs, scroll_id, nil
}

func IndexMetric(m *schema.MetricDefinition) error {
	if err := m.Validate(); err != nil {
		return err
	}

	if time.Since(startTime) < (time.Duration(warmUpDuration) * time.Second) {
		// we are in our warmup period.
		if rand.Intn(100) > warmUpPercent {
			return nil
		}
	}
	log.Debug("indexing %s in elasticsearch", m.Id)
	err := Indexer.Index(IndexName, "metric_index", m.Id, "", "", nil, m)
	if err != nil {
		log.Error(3, "failed to send payload to BulkApi indexer. %s", err)
		return err
	}

	return nil
}

func GetMetricDefinition(id string) (*schema.MetricDefinition, bool, error) {
	if id == "" {
		panic("key cant be empty string.")
	}
	res, err := es.Get(IndexName, "metric_index", id, nil)
	if err != nil {
		if err == elastigo.RecordNotFound {
			log.Debug("%s not in ES. %s", id, err)
			return nil, false, nil
		} else {
			log.Error(3, "elasticsearch query failed. %s", err)
			return nil, false, err
		}
	}
	def, err := schema.MetricDefinitionFromJSON(*res.Source)
	return def, true, err
}
