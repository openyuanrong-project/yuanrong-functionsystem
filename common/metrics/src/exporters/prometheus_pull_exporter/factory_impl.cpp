/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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

#include <memory>
#include <new>

#include "metrics/exporters/prometheus_pull_exporter/prometheus_pull_exporter.h"
#include "metrics/plugin/exporter_handle.h"
#include "metrics/plugin/hook.h"

namespace observability::exporters::metrics {

class ExporterHandle final : public observability::plugin::metrics::ExporterHandle {
public:
    explicit ExporterHandle(std::shared_ptr<PrometheusPullExporter> &&exporter) noexcept : exporter_(exporter)
    {
    }
    ~ExporterHandle() override = default;

    observability::exporters::metrics::Exporter &Exporter() const noexcept override
    {
        return *exporter_;
    }

private:
    std::shared_ptr<PrometheusPullExporter> exporter_;
};

class FactoryImpl final : public observability::plugin::metrics::Factory::FactoryImpl {
public:
    std::unique_ptr<observability::plugin::metrics::ExporterHandle> MakeExporterHandle(
        std::string exporterConfig, std::unique_ptr<char[]> &) const noexcept override
    {
        std::shared_ptr<PrometheusPullExporter> exporter = std::make_shared<PrometheusPullExporter>(exporterConfig);
        return std::unique_ptr<ExporterHandle>{ new (std::nothrow) ExporterHandle(std::move(exporter)) };
    }
};

static std::unique_ptr<observability::plugin::metrics::Factory::FactoryImpl> MakeFactoryImpl(
    std::unique_ptr<char[]>& /* error */) noexcept
{
    return std::unique_ptr<observability::plugin::metrics::Factory::FactoryImpl>{ new (std::nothrow) FactoryImpl{} };
}

OBSERVABILITY_DEFINE_PLUGIN_HOOK(MakeFactoryImpl);
}  // namespace observability::exporters::metrics
