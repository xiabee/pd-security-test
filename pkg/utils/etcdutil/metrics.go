// Copyright 2023 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package etcdutil

import "github.com/prometheus/client_golang/prometheus"

var (
	sourceLabel   = "source"
	typeLabel     = "type"
	endpointLabel = "endpoint"
)

var (
	etcdStateGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "server",
			Name:      "etcd_client",
			Help:      "Etcd client states.",
		}, []string{sourceLabel, typeLabel})

	etcdEndpointLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "server",
			Name:      "etcd_endpoint_latency_seconds",
			Help:      "Bucketed histogram of latency of health check.",
			Buckets:   []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		}, []string{sourceLabel, endpointLabel})
)

func init() {
	prometheus.MustRegister(etcdStateGauge)
	prometheus.MustRegister(etcdEndpointLatency)
}
