/*
 * Copyright (c) Meta Platforms, Inc. and its affiliates.
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

#include "axiom/connectors/file/core/FileTable.h"

#include <numeric>

#include "velox/common/base/Exceptions.h"
#include "velox/common/serialization/Serializable.h"

namespace facebook::axiom::connector::file {

using namespace facebook::velox;

folly::dynamic FileSplit::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = "FileSplit";
  obj["connectorId"] = connectorId;
  obj["filePath"] = filePath;
  return obj;
}

std::shared_ptr<FileSplit> FileSplit::create(const folly::dynamic& obj) {
  return std::make_shared<FileSplit>(
      obj["connectorId"].asString(), obj["filePath"].asString());
}

void FileSplit::registerSerDe() {
  DeserializationRegistryForSharedPtr().Register("FileSplit", FileSplit::create);
}

folly::dynamic FileColumnHandle::serialize() const {
  auto obj = serializeBase("FileColumnHandle");
  obj["columnName"] = name_;
  return obj;
}

velox::connector::ColumnHandlePtr FileColumnHandle::create(
    const folly::dynamic& obj,
    void* /*context*/) {
  return std::make_shared<FileColumnHandle>(obj["columnName"].asString());
}

void FileColumnHandle::registerSerDe() {
  DeserializationWithContextRegistryForSharedPtr().Register(
      "FileColumnHandle", FileColumnHandle::create);
}

folly::dynamic FileTableHandle::serialize() const {
  auto obj = serializeBase("FileTableHandle");
  obj["schema"] = schemaTableName_.schema;
  obj["table"] = schemaTableName_.table;
  obj["filePath"] = filePath_;
  obj["filePaths"] = ISerializable::serialize(filePaths_);
  obj["suffix"] = suffix_;
  obj["fullSchema"] = fullSchema_->serialize();
  folly::dynamic columns = folly::dynamic::array;
  for (const auto& column : columnHandles_) {
    columns.push_back(column->name());
  }
  obj["columns"] = std::move(columns);
  return obj;
}

velox::connector::ConnectorTableHandlePtr FileTableHandle::create(
    const folly::dynamic& obj,
    void* context) {
  std::vector<velox::connector::ColumnHandlePtr> columns;
  for (const auto& column : obj["columns"]) {
    columns.push_back(std::make_shared<FileColumnHandle>(column.asString()));
  }
  return std::make_shared<FileTableHandle>(
      obj["connectorId"].asString(),
      SchemaTableName{obj["schema"].asString(), obj["table"].asString()},
      obj["filePath"].asString(),
      ISerializable::deserialize<std::vector<std::string>>(obj["filePaths"]),
      obj["suffix"].asString(),
      ISerializable::deserialize<RowType>(obj["fullSchema"], context),
      std::move(columns));
}

void FileTableHandle::registerSerDe() {
  DeserializationWithContextRegistryForSharedPtr().Register(
      "FileTableHandle", FileTableHandle::create);
}

bool FileTable::isValidName(std::string_view name) {
  return !name.empty() && name.find_first_of("$./") == std::string_view::npos;
}

std::pair<std::string, std::string> FileTable::parseName(
    const std::string& tableName) {
  auto pos = tableName.rfind('$');
  if (pos != std::string::npos && pos > 0) {
    auto suffix = tableName.substr(pos + 1);
    if (isValidName(suffix)) {
      return {tableName.substr(0, pos), std::move(suffix)};
    }
  }
  return {tableName, ""};
}

FileTableLayout::FileTableLayout(
    Table* table,
    velox::connector::Connector* connector,
    std::vector<const Column*> columns,
    std::string filePath,
    std::vector<std::string> filePaths,
    std::string suffix)
    : TableLayout(
          table->name().schema,
          table,
          connector,
          std::move(columns),
          /*partitionColumns=*/{},
          /*orderColumns=*/{},
          /*sortOrder=*/{},
          /*lookupKeys=*/{},
          /*supportsScan=*/true),
      filePath_(std::move(filePath)),
      filePaths_(std::move(filePaths)),
      suffix_(std::move(suffix)) {}

velox::connector::ColumnHandlePtr FileTableLayout::createColumnHandle(
    const ConnectorSessionPtr& /*session*/,
    const std::string& columnName,
    std::vector<velox::common::Subfield> /*subfields*/,
    std::optional<TypePtr> /*castToType*/,
    SubfieldMapping /*subfieldMapping*/) const {
  auto column = findColumn(columnName);
  VELOX_CHECK_NOT_NULL(column, "Column not found: {}", columnName);
  return std::make_shared<FileColumnHandle>(columnName);
}

velox::connector::ConnectorTableHandlePtr FileTableLayout::createTableHandle(
    const ConnectorSessionPtr& /*session*/,
    std::vector<velox::connector::ColumnHandlePtr> columnHandles,
    core::ExpressionEvaluator& /*evaluator*/,
    std::vector<core::TypedExprPtr> filters,
    std::vector<int32_t>& rejectedFilterIndices,
    RowTypePtr /*dataColumns*/,
    std::optional<LookupKeys> /*lookupKeys*/) const {
  rejectedFilterIndices.resize(filters.size());
  std::iota(rejectedFilterIndices.begin(), rejectedFilterIndices.end(), 0);
  return std::make_shared<FileTableHandle>(
      connectorId(),
      table().name(),
      filePath_,
      filePaths_,
      suffix_,
      table().type(),
      std::move(columnHandles));
}

FileTable::FileTable(
    const SchemaTableName& tableName,
    const RowTypePtr& schema,
    velox::connector::Connector* connector,
    std::string filePath,
    std::vector<std::string> filePaths,
    std::string suffix)
    : Table(tableName, makeColumns(schema)) {
  layout_ = std::make_unique<FileTableLayout>(
      this,
      connector,
      allColumns(),
      std::move(filePath),
      std::move(filePaths),
      std::move(suffix));
  layouts_.push_back(layout_.get());
}

} // namespace facebook::axiom::connector::file
