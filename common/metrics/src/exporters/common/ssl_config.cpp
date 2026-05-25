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
#include "metrics/exporters/common/ssl_config.h"

#include <array>
#include <cctype>
#include <string>

#include <nlohmann/json.hpp>

#include "metrics/exporters/common/sensitive_data.h"

namespace observability::exporters::metrics {
namespace {
constexpr size_t BASE64_TABLE_SIZE = 256;
constexpr int ALPHABET_SIZE = 26;
constexpr int DIGIT_SIZE = 10;
constexpr int LOWER_CASE_OFFSET = 26;
constexpr int DIGIT_OFFSET = 52;
constexpr int PLUS_OFFSET = 62;
constexpr int SLASH_OFFSET = 63;
constexpr int BITS_PER_BASE64_CHAR = 6;
constexpr int BITS_PER_BYTE = 8;
constexpr int BYTE_MASK = (1 << BITS_PER_BYTE) - 1;

constexpr std::array<int, BASE64_TABLE_SIZE> BuildBase64DecodeTable()
{
    std::array<int, BASE64_TABLE_SIZE> table {};
    for (auto &item : table) {
        item = -1;
    }
    for (int i = 0; i < ALPHABET_SIZE; ++i) {
        table[static_cast<size_t>('A' + i)] = i;
        table[static_cast<size_t>('a' + i)] = i + LOWER_CASE_OFFSET;
    }
    for (int i = 0; i < DIGIT_SIZE; ++i) {
        table[static_cast<size_t>('0' + i)] = i + DIGIT_OFFSET;
    }
    table[static_cast<size_t>('+')] = PLUS_OFFSET;
    table[static_cast<size_t>('/')] = SLASH_OFFSET;
    return table;
}

std::string DecodeBase64(const std::string &input)
{
    static constexpr auto table = BuildBase64DecodeTable();
    std::string output;
    int val = 0;
    int valb = -BITS_PER_BYTE;
    for (const auto ch : input) {
        const auto uch = static_cast<unsigned char>(ch);
        if (std::isspace(uch)) {
            continue;
        }
        if (ch == '=') {
            break;
        }
        const int decoded = table[uch];
        if (decoded < 0) {
            return "";
        }
        val = (val << BITS_PER_BASE64_CHAR) + decoded;
        valb += BITS_PER_BASE64_CHAR;
        if (valb >= 0) {
            output.push_back(static_cast<char>((val >> valb) & BYTE_MASK));
            valb -= BITS_PER_BYTE;
        }
    }
    return output;
}

std::string DecodeCertData(const nlohmann::json &configJson, const std::string &key)
{
    if (configJson.find(key) == configJson.end()) {
        return "";
    }
    const auto data = configJson.at(key).get<std::string>();
    return DecodeBase64(data);
}
}  // namespace

void SSLConfig::Parse(const std::string &config)
{
    try {
        auto configJson = nlohmann::json::parse(config);
        if (configJson.find("isSSLEnable") != configJson.end()) {
            isSSLEnable_ = configJson.at("isSSLEnable");
        }
        if (isSSLEnable_ && configJson.find("rootCertFile") != configJson.end()) {
            rootCertFile_ = configJson.at("rootCertFile");
        }
        if (isSSLEnable_ && configJson.find("certFile") != configJson.end()) {
            certFile_ = configJson.at("certFile");
        }
        if (isSSLEnable_ && configJson.find("keyFile") != configJson.end()) {
            keyFile_ = configJson.at("keyFile");
        }
        if (isSSLEnable_) {
            rootCertData_ = DecodeCertData(configJson, "rootCertData");
            certData_ = DecodeCertData(configJson, "certData");
            keyData_ = DecodeCertData(configJson, "keyData");
        }
        if (isSSLEnable_ && configJson.find("passphrase") != configJson.end()) {
            passphrase_ = configJson.at("passphrase");
        }
    } catch (std::exception &e) {
        std::cerr << "failed to parse PrometheusPushExportOptions" << std::endl;
    }
}
}  // namespace observability::exporters::metrics
