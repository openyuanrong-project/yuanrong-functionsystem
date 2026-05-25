/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "metrics/exporters/common/prometheus_text_serializer.h"

namespace observability {

namespace exporters::metrics {

void SerializeContentValue(std::ostream &ost, const observability::sdk::metrics::PointData &point)
{
    std::visit([&ost](auto &&arg) { ost << arg; }, point.value);
}

void WriteStringValue(std::ostream &ost, const std::string &value)
{
    for (auto c : value) {
        switch (c) {
            case '\n':
                ost << '\\' << 'n';
                break;
            case '\\':
                ost << '\\' << c;
                break;
            case '"':
                ost << '\\' << c;
                break;
            default:
                ost << c;
                break;
        }
    }
}

void SerializeContentHead(std::ostream &ost, const observability::sdk::metrics::MetricData &metric,
                          const observability::sdk::metrics::PointData &point)
{
    ost << metric.instrumentDescriptor.name;
    if (point.labels.size() != 0) {
        ost << "{";
        std::string prefix = "";
        for (auto &label : point.labels) {
            ost << prefix << label.first << "=\"";
            WriteStringValue(ost, label.second);
            ost << "\"";
            prefix = ",";
        }
        ost << "}";
    }
    ost << " ";
}

void SerializeSimpleMetric(std::ostream &ost, const observability::sdk::metrics::MetricData &metric,
                           const observability::sdk::metrics::PointData point)
{
    SerializeContentHead(ost, metric, point);
    SerializeContentValue(ost, point);
    ost << "\n";
}

void SerializeMetricByType(std::ostream &ost, const observability::sdk::metrics::MetricData &metric)
{
    switch (metric.instrumentDescriptor.type) {
        case observability::sdk::metrics::InstrumentType::COUNTER:
            ost << "# TYPE " << metric.instrumentDescriptor.name << " counter\n";
            for (auto &point : metric.pointData) {
                SerializeSimpleMetric(ost, metric, point);
            }
            break;

        case observability::sdk::metrics::InstrumentType::GAUGE:
            ost << "# TYPE " << metric.instrumentDescriptor.name << " gauge\n";
            for (auto &point : metric.pointData) {
                SerializeSimpleMetric(ost, metric, point);
            }
            break;
        case observability::sdk::metrics::InstrumentType::HISTOGRAM:
            ost << "# TYPE " << metric.instrumentDescriptor.name << " histogram\n";
            break;
        default:
            break;
    }
}

void SerializeHelp(std::ostream &ost, const observability::sdk::metrics::MetricData &metric)
{
    if (!metric.instrumentDescriptor.description.empty()) {
        ost << "# HELP " << metric.instrumentDescriptor.name << " " << metric.instrumentDescriptor.description << "\n";
    }
}

void SerializeMetric(std::ostream &ost, const observability::sdk::metrics::MetricData &metric)
{
    SerializeHelp(ost, metric);
    SerializeMetricByType(ost, metric);
}

void PrometheusTextSerializer::Serialize(std::ostream &ost, const observability::sdk::metrics::MetricData &metric) const
{
    auto savedLocale = ost.getloc();
    auto savedPrecision = ost.precision();
    (void)ost.imbue(std::locale::classic());
    (void)ost.precision(std::numeric_limits<double>::max_digits10 - 1);

    SerializeMetric(ost, metric);

    if (ost.good()) {
        (void)ost.flush();
    }

    (void)ost.imbue(savedLocale);
    (void)ost.precision(savedPrecision);
}

}  // namespace exporters::metrics
}  // namespace observability
