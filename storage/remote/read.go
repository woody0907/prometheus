// Copyright 2017 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package remote

import (
	"bytes"
	"fmt"
	"net/http"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/metric"
)

// Reader reads from a remote data source.
type Reader struct {
	url    string
	client http.Client
}

// NewReader creates a new reader.
func NewReader(url string) *Reader {
	return &Reader{
		url: url,
		client: http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (r *Reader) Read(from, through model.Time, matchers ...metric.LabelMatchers) (model.Matrix, error) {
	req := &ReadRequest{
		StartTimestampMs: int64(from),
		EndTimestampMs:   int64(through),
		MatchersSet:      labelMatchersToProto(matchers...),
	}

	data, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("unable to marshal read request: %v", err)
	}

	buf := bytes.NewBuffer(data)
	// TODO: POST is weird for a query, but as per http://stackoverflow.com/questions/978061/http-get-with-request-body
	// it's not valid to attach meaning to an HTTP body in a GET request.
	httpReq, err := http.NewRequest("POST", r.url, buf)
	if err != nil {
		return nil, fmt.Errorf("unable to create request: %v", err)
	}
	httpResp, err := r.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("error sending request: %v", err)
	}
	defer httpResp.Body.Close()
	if httpResp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("server returned HTTP status %s", httpResp.Status)
	}

	buf.Reset()
	_, err = buf.ReadFrom(httpResp.Body)
	if err != nil {
		return nil, fmt.Errorf("unable to read response body: %v", err)
	}

	var resp ReadResponse
	err = proto.Unmarshal(buf.Bytes(), &resp)
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshal response body: %v", err)
	}

	return matrixFromProto(resp.Timeseries), nil
}

func labelMatchersToProto(matchersSet ...metric.LabelMatchers) []*LabelMatchers {
	result := make([]*LabelMatchers, 0, len(matchersSet))
	for _, matchers := range matchersSet {
		pbMatchers := &LabelMatchers{
			Matchers: make([]*LabelMatcher, 0, len(matchers)),
		}
		for _, m := range matchers {
			var mType MatchType
			switch m.Type {
			case metric.Equal:
				mType = MatchType_EQUAL
			case metric.NotEqual:
				mType = MatchType_NOT_EQUAL
			case metric.RegexMatch:
				mType = MatchType_REGEX_MATCH
			case metric.RegexNoMatch:
				mType = MatchType_REGEX_NO_MATCH
			default:
				panic("invalid matcher type")
			}
			pbMatchers.Matchers = append(pbMatchers.Matchers, &LabelMatcher{
				Type:  mType,
				Name:  string(m.Name),
				Value: string(m.Value),
			})
		}
		result = append(result, pbMatchers)
	}
	return result
}

func matrixFromProto(seriesSet []*TimeSeries) model.Matrix {
	m := make(model.Matrix, 0, len(seriesSet))
	for _, ts := range seriesSet {
		var ss model.SampleStream
		ss.Metric = labelPairsToMetric(ts.Labels)
		ss.Values = make([]model.SamplePair, 0, len(ts.Samples))
		for _, s := range ts.Samples {
			ss.Values = append(ss.Values, model.SamplePair{
				Value:     model.SampleValue(s.Value),
				Timestamp: model.Time(s.TimestampMs),
			})
		}
		m = append(m, &ss)
	}

	return m
}

func labelPairsToMetric(labelPairs []*LabelPair) model.Metric {
	metric := make(model.Metric, len(labelPairs))
	for _, l := range labelPairs {
		metric[model.LabelName(l.Name)] = model.LabelValue(l.Value)
	}
	return metric
}
