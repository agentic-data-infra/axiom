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

#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/tests/QueryTestBase.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace velox;
namespace lp = facebook::axiom::logical_plan;

// A range partitioning the planner places; the tests do not run it.
class TestRangePartitionType : public connector::PartitionType {
 public:
  TestRangePartitionType(int32_t numPartitions, TypePtr keyType)
      : numPartitions_{numPartitions}, keyTypes_{std::move(keyType)} {}

  std::shared_ptr<connector::PartitionType> copartition(
      const connector::PartitionType& /*other*/) const override {
    return nullptr;
  }

  std::shared_ptr<connector::PartitionType> scaleDown(
      int32_t /*maxPartitions*/) const override {
    VELOX_UNSUPPORTED();
  }

  core::PartitionFunctionSpecPtr makeSpec(
      const std::vector<column_index_t>& /*channels*/,
      const std::vector<VectorPtr>& /*constants*/,
      bool /*isLocal*/) const override {
    return std::make_shared<core::GatherPartitionFunctionSpec>();
  }

  int32_t numPartitions() const override {
    return numPartitions_;
  }

  const std::vector<TypePtr>& keyTypes() const override {
    return keyTypes_;
  }

  int32_t numGroups() const override {
    return numPartitions_;
  }

  std::string toString() const override {
    return fmt::format("range({})", numPartitions_);
  }

 private:
  const int32_t numPartitions_;
  const std::vector<TypePtr> keyTypes_;
};

class SortPartitionerTest : public test::QueryTestBase {
 protected:
  static constexpr int32_t kRanges = 3;

  void SetUp() override {
    test::QueryTestBase::SetUp();
    testConnector_->addTable("t", ROW({"a", "b"}, {VARCHAR(), BIGINT()}));
    testConnector_->addTable("w", ROW({"a", "b"}, {VARCHAR(), BIGINT()}));
  }

  // Plans 'logicalPlan' on 4 workers, with 'kRanges' ranges for a global sort
  // if 'rangeSort'.
  test::PlannedQuery planSort(
      const lp::LogicalPlanNodePtr& logicalPlan,
      bool rangeSort,
      bool remoteOutput) {
    MultiFragmentPlan::Options options{
        .maxRemotePartitions = 4,
        .maxLocalPartitions = 2,
        .remoteOutput = remoteOutput,
    };
    if (rangeSort) {
      options.sortPartitioner = [](const Type& keyType,
                                   bool /*ascending*/,
                                   bool /*nullsFirst*/,
                                   int32_t /*numPartitions*/)
          -> std::shared_ptr<const connector::PartitionType> {
        VELOX_CHECK(keyType.isVarchar());
        return std::make_shared<TestRangePartitionType>(kRanges, VARCHAR());
      };
    }
    return planVelox(logicalPlan, options);
  }

  // The first stage whose plan has a node of type 'T', or nullptr.
  template <typename T>
  static const core::Stage* findStage(const MultiFragmentPlan& plan) {
    for (const auto& stage : plan.stages()) {
      if (core::PlanNode::findFirstNode(
              stage.plan.get(), [](const core::PlanNode* node) {
                return dynamic_cast<const T*>(node) != nullptr;
              })) {
        return &stage;
      }
    }
    return nullptr;
  }
};

// The sort under a write sorts each range in its own writer task: one task per
// range, sized from the range exchange below the sort, and no merge.
TEST_F(SortPartitionerTest, sortedWrite) {
  auto logicalPlan = lp::PlanBuilder(makeContext())
                         .tableScan("t")
                         .sort({"a"})
                         .tableWrite("w", lp::WriteKind::kInsert, {"a", "b"})
                         .build();

  auto ranged =
      planSort(logicalPlan, /*rangeSort=*/true, /*remoteOutput=*/false);
  const auto* writer = findStage<core::TableWriteNode>(*ranged.plan);
  ASSERT_NE(writer, nullptr);
  EXPECT_EQ(writer->type, core::StageType::kFixed);
  EXPECT_EQ(writer->numRemotePartitions, kRanges);
  EXPECT_EQ(findStage<core::MergeExchangeNode>(*ranged.plan), nullptr);

  auto merged =
      planSort(logicalPlan, /*rangeSort=*/false, /*remoteOutput=*/false);
  EXPECT_NE(findStage<core::MergeExchangeNode>(*merged.plan), nullptr);
}

// The writer is sized from the range exchange even when a projection above the
// sort drops the sort key.
TEST_F(SortPartitionerTest, sortedWriteWithoutSortKey) {
  testConnector_->addTable("v", ROW("b", BIGINT()));
  auto logicalPlan = lp::PlanBuilder(makeContext())
                         .tableScan("t")
                         .sort({"a"})
                         .project({"b"})
                         .tableWrite("v", lp::WriteKind::kInsert, {"b"})
                         .build();

  auto ranged =
      planSort(logicalPlan, /*rangeSort=*/true, /*remoteOutput=*/false);
  const auto* writer = findStage<core::TableWriteNode>(*ranged.plan);
  ASSERT_NE(writer, nullptr);
  EXPECT_EQ(writer->numRemotePartitions, kRanges);
}

// With remote output, a sort at the root runs one task per range, which the
// caller reads in order.
TEST_F(SortPartitionerTest, sortedResult) {
  auto logicalPlan =
      lp::PlanBuilder(makeContext()).tableScan("t").sort({"a"}).build();

  auto ranged =
      planSort(logicalPlan, /*rangeSort=*/true, /*remoteOutput=*/true);
  const auto& root = ranged.plan->stagePlan().root();
  EXPECT_EQ(root.type, core::StageType::kFixed);
  EXPECT_EQ(root.numRemotePartitions, kRanges);
  EXPECT_EQ(findStage<core::MergeExchangeNode>(*ranged.plan), nullptr);

  // A result gathered onto one task merges the sorted streams.
  auto gathered =
      planSort(logicalPlan, /*rangeSort=*/true, /*remoteOutput=*/false);
  EXPECT_NE(findStage<core::MergeExchangeNode>(*gathered.plan), nullptr);
}

} // namespace
} // namespace facebook::axiom::optimizer
