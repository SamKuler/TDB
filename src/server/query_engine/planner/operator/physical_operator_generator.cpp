#include "include/query_engine/planner/operator/physical_operator_generator.h"

#include <cmath>
#include <utility>
#include "include/query_engine/planner/node/logical_node.h"
#include "include/query_engine/planner/operator/physical_operator.h"
#include "include/query_engine/planner/node/table_get_logical_node.h"
#include "include/query_engine/planner/operator/table_scan_physical_operator.h"
#include "include/query_engine/planner/node/predicate_logical_node.h"
#include "include/query_engine/planner/operator/predicate_physical_operator.h"
#include "include/query_engine/planner/node/order_by_logical_node.h"
#include "include/query_engine/planner/operator/order_physical_operator.h"
#include "include/query_engine/planner/node/project_logical_node.h"
#include "include/query_engine/planner/operator/project_physical_operator.h"
#include "include/query_engine/planner/node/aggr_logical_node.h"
#include "include/query_engine/planner/operator/aggr_physical_operator.h"
#include "include/query_engine/planner/node/insert_logical_node.h"
#include "include/query_engine/planner/operator/insert_physical_operator.h"
#include "include/query_engine/planner/node/delete_logical_node.h"
#include "include/query_engine/planner/operator/delete_physical_operator.h"
#include "include/query_engine/planner/node/update_logical_node.h"
#include "include/query_engine/planner/operator/update_physical_operator.h"
#include "include/query_engine/planner/node/explain_logical_node.h"
#include "include/query_engine/planner/operator/explain_physical_operator.h"
#include "include/query_engine/planner/node/join_logical_node.h"
#include "include/query_engine/planner/operator/join_physical_operator.h"
#include "include/query_engine/planner/node/group_by_logical_node.h"
#include "include/query_engine/planner/operator/group_by_physical_operator.h"

#include "include/query_engine/planner/operator/index_scan_physical_operator.h"


#include "include/query_engine/structor/expression/comparison_expression.h"
#include "include/query_engine/structor/expression/field_expression.h"
#include "include/query_engine/structor/expression/value_expression.h"

#include "common/log/log.h"
#include "include/storage_engine/recorder/table.h"

using namespace std;

RC PhysicalOperatorGenerator::create(LogicalNode &logical_operator, unique_ptr<PhysicalOperator> &oper, bool is_delete)
{
  switch (logical_operator.type()) {
    case LogicalNodeType::TABLE_GET: {
      return create_plan(static_cast<TableGetLogicalNode &>(logical_operator), oper, is_delete);
    }

    case LogicalNodeType::PREDICATE: {
      return create_plan(static_cast<PredicateLogicalNode &>(logical_operator), oper, is_delete);
    }

    case LogicalNodeType::ORDER: {
      return create_plan(static_cast<OrderByLogicalNode &>(logical_operator), oper);
    }

    case LogicalNodeType::PROJECTION: {
      return create_plan(static_cast<ProjectLogicalNode &>(logical_operator), oper, is_delete);
    }

    case LogicalNodeType::AGGR: {
      return create_plan(static_cast<AggrLogicalNode &>(logical_operator), oper);
    }

    case LogicalNodeType::INSERT: {
      return create_plan(static_cast<InsertLogicalNode &>(logical_operator), oper);
    }

    case LogicalNodeType::DELETE: {
      return create_plan(static_cast<DeleteLogicalNode &>(logical_operator), oper);
    }

    case LogicalNodeType::UPDATE: {
      return create_plan(static_cast<UpdateLogicalNode &>(logical_operator), oper);
    }

    case LogicalNodeType::EXPLAIN: {
      return create_plan(static_cast<ExplainLogicalNode &>(logical_operator), oper, is_delete);
    }
    // TODO [Lab3] 实现JoinNode到JoinOperator的转换
    case LogicalNodeType::JOIN: {
      return create_plan(static_cast<JoinLogicalNode &>(logical_operator), oper);
    }
    case LogicalNodeType::GROUP_BY: {
      return RC::UNIMPLENMENT;
    }

    default: {
      return RC::INVALID_ARGUMENT;
    }
  }
}

// TODO [Lab2] 
// 在原有的实现中，会直接生成TableScanOperator对所需的数据进行全表扫描，但其实在生成执行计划时，我们可以进行简单的优化：
// 首先检查扫描的table是否存在索引，如果存在可以使用的索引，那么我们可以直接生成IndexScanOperator来减少磁盘的扫描
RC PhysicalOperatorGenerator::create_plan(
    TableGetLogicalNode &table_get_oper, unique_ptr<PhysicalOperator> &oper, bool is_delete)
{
  vector<unique_ptr<Expression>> &predicates = table_get_oper.predicates();
  Index *index = nullptr;
  Table *table = table_get_oper.table();

  std::map<std::string, std::pair<const FieldExpr*, const ValueExpr*>> field_value_map;

  // TODO [Lab2] 生成IndexScanOperator的准备工作,主要包含:
  // 1. 通过predicates获取具体的值表达式， 目前应该只支持等值表达式的索引查找
    // example:
    //  if(predicate.type == ExprType::COMPARE){
    //   auto compare_expr = dynamic_cast<ComparisonExpr*>(predicate.get());
    //   if(compare_expr->comp() != EQUAL_TO) continue;
    //   [process]
    //  }
  // 2. 对应上面example里的process阶段， 找到等值表达式中对应的FieldExpression和ValueExpression(左值和右值)
  // 通过FieldExpression找到对应的Index, 通过ValueExpression找到对应的Value
  std::vector<Value> values;
  for (auto &predicate : predicates){
    if (predicate->type() == ExprType::COMPARISON){
      auto compare_expr = dynamic_cast<ComparisonExpr*>(predicate.get());
      if (compare_expr->comp() == EQUAL_TO)
      {
        Expression *left = compare_expr->left().get();
        Expression *right = compare_expr->right().get();

        FieldExpr *field_expr = nullptr;
        ValueExpr *value_expr = nullptr;

        // 找到等值表达式中对应的FieldExpression和ValueExpression
        if(left->type()==ExprType::FIELD&&right->type()==ExprType::VALUE)
        {
          field_expr = dynamic_cast<FieldExpr*>(left);
          value_expr = dynamic_cast<ValueExpr*>(right);
        }
        else if(right->type()==ExprType::FIELD&&left->type()==ExprType::VALUE)
        {// 不确定左右值
          field_expr = dynamic_cast<FieldExpr*>(right);
          value_expr = dynamic_cast<ValueExpr*>(left);
        }

        //通过FieldExpression找到对应的Index, 通过ValueExpression找到对应的Value
        if(field_expr!=nullptr&&value_expr!=nullptr)
        {
          field_value_map[field_expr->field_name()] = std::make_pair(field_expr, value_expr);
        }
      }
    }
  }

  // 最查询table的所有索引，找到最符合的索引
  const TableMeta &table_meta = table->table_meta();
  const int index_count = table_meta.index_num();
  IndexMeta *best_fit_index_meta = nullptr;
  for (int i = 0; i < index_count; i++) {
    const IndexMeta *index_meta = table_meta.index(i);
    bool accept = true;
    int filed_count = index_meta->field_amount();
    for (int j = 0; j < filed_count; j++) {
      const char *field_name = index_meta->field(j);
      auto it = field_value_map.find(field_name);
      if (it == field_value_map.end()) {
        accept=false;
      }
    }
    if(accept==false)
    {
      continue;
    }
    // 最优选择
    if (best_fit_index_meta == nullptr ||
        best_fit_index_meta->field_amount() < index_meta->field_amount()) {
      best_fit_index_meta = const_cast<IndexMeta *>(index_meta);
    }
  }
  // 构建查询的值
  if (best_fit_index_meta != nullptr) {
    index = table->find_index(best_fit_index_meta->name());
    ASSERT(index != nullptr, "failed to find index %s", best_fit_index_meta->name());

    //构建要查询的值，由于是多字段，所以需要重新创建一个Value对象
    for (int i=0;i<best_fit_index_meta->field_amount();i++){
      const char *field_name = best_fit_index_meta->field(i);
      auto &value = field_value_map[field_name].second->get_value();
      values.emplace_back(value);
    }
  }
  // if(best_fit_index_meta!=nullptr){
  //   if(best_fit_index_meta->field_amount()>1)
  //   {
  //     value.set_type(AttrType::CHARS);
  //   }
  //   else
  //   {
  //     value.set_type(table->table_meta().field(best_fit_index_meta->field(0))->type());
  //   }
  // }

  if(index == nullptr){
    auto table_scan_oper = new TableScanPhysicalOperator(table, table_get_oper.table_alias(), table_get_oper.readonly());
    table_scan_oper->isdelete_ = is_delete;
    table_scan_oper->set_predicates(std::move(predicates));
    oper = unique_ptr<PhysicalOperator>(table_scan_oper);
    LOG_TRACE("use table scan");
  }else{
    // TODO [Lab2] 生成IndexScanOperator, 并放置在算子树上，下面是一个实现参考，具体实现可以根据需要进行修改
    // IndexScanner 在设计时，考虑了范围查找索引的情况，但此处我们只需要考虑单个键的情况
    // const Value &value = value_expression->get_value();
    // IndexScanPhysicalOperator *operator =
    //              new IndexScanPhysicalOperator(table, index, readonly, &value, true, &value, true);
    // oper = unique_ptr<PhysicalOperator>(operator);
    auto index_scan_oper = new IndexScanPhysicalOperator(table, index, table_get_oper.readonly(),&values, true, &values, true);
    index_scan_oper->isdelete_ = is_delete;
    index_scan_oper->set_table_alias(table_get_oper.table_alias());
    index_scan_oper->set_predicates(std::move(predicates));
    oper = unique_ptr<PhysicalOperator>(index_scan_oper);
    LOG_TRACE("use index scan on table %s",table->name());
  }

  return RC::SUCCESS;
}

RC PhysicalOperatorGenerator::create_plan(
    PredicateLogicalNode &pred_oper, unique_ptr<PhysicalOperator> &oper, bool is_delete)
{
  vector<unique_ptr<LogicalNode>> &children_opers = pred_oper.children();
  ASSERT(children_opers.size() == 1, "predicate logical operator's sub oper number should be 1");

  LogicalNode &child_oper = *children_opers.front();

  unique_ptr<PhysicalOperator> child_phy_oper;
  RC rc = create(child_oper, child_phy_oper, is_delete);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create child operator of predicate operator. rc=%s", strrc(rc));
    return rc;
  }

  vector<unique_ptr<Expression>> &expressions = pred_oper.expressions();
  ASSERT(expressions.size() == 1, "predicate logical operator's children should be 1");

  unique_ptr<Expression> expression = std::move(expressions.front());

  oper = unique_ptr<PhysicalOperator>(new PredicatePhysicalOperator(std::move(expression)));
  oper->add_child(std::move(child_phy_oper));
  oper->isdelete_ = is_delete;
  return rc;
}

RC PhysicalOperatorGenerator::create_plan(AggrLogicalNode &aggr_oper, unique_ptr<PhysicalOperator> &oper)
{
  vector<unique_ptr<LogicalNode>> &child_opers = aggr_oper.children();

  unique_ptr<PhysicalOperator> child_phy_oper;

  RC rc = RC::SUCCESS;
  if (!child_opers.empty()) {
    LogicalNode *child_oper = child_opers.front().get();
    rc = create(*child_oper, child_phy_oper);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to create project logical operator's child physical operator. rc=%s", strrc(rc));
      return rc;
    }
  }

  auto *aggr_operator = new AggrPhysicalOperator(&aggr_oper);

  if (child_phy_oper) {
    aggr_operator->add_child(std::move(child_phy_oper));
  }

  oper = unique_ptr<PhysicalOperator>(aggr_operator);

  LOG_TRACE("create a project physical operator");
  return rc;
}

RC PhysicalOperatorGenerator::create_plan(OrderByLogicalNode &order_oper, unique_ptr<PhysicalOperator> &oper)
{
  vector<unique_ptr<LogicalNode>> &child_opers = order_oper.children();

  unique_ptr<PhysicalOperator> child_phy_oper;

  RC rc = RC::SUCCESS;
  if (!child_opers.empty()) {
    LogicalNode *child_oper = child_opers.front().get();
    rc = create(*child_oper, child_phy_oper);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to create project logical operator's child physical operator. rc=%s", strrc(rc));
      return rc;
    }
  }

  OrderPhysicalOperator* order_operator = new OrderPhysicalOperator(std::move(order_oper.order_units()));

  if (child_phy_oper) {
    order_operator->add_child(std::move(child_phy_oper));
  }

  oper = unique_ptr<PhysicalOperator>(order_operator);

  LOG_TRACE("create a project physical operator");
  return rc;
}

RC PhysicalOperatorGenerator::create_plan(
    ProjectLogicalNode &project_oper, unique_ptr<PhysicalOperator> &oper, bool is_delete)
{
  vector<unique_ptr<LogicalNode>> &child_opers = project_oper.children();

  unique_ptr<PhysicalOperator> child_phy_oper;

  RC rc = RC::SUCCESS;
  if (!child_opers.empty()) {
    LogicalNode *child_oper = child_opers.front().get();
    rc = create(*child_oper, child_phy_oper, is_delete);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to create project logical operator's child physical operator. rc=%s", strrc(rc));
      return rc;
    }
  }

  auto *project_operator = new ProjectPhysicalOperator(&project_oper);
  for (const auto &i : project_oper.expressions()) {
    // TupleCellSpec 的构造函数中已经 copy 了 Expression，这里无需 copy
    project_operator->add_projector(i.get());
  }

  if (child_phy_oper) {
    project_operator->add_child(std::move(child_phy_oper));
  }

  oper = unique_ptr<PhysicalOperator>(project_operator);
  oper->isdelete_ = is_delete;

  LOG_TRACE("create a project physical operator");
  return rc;
}

RC PhysicalOperatorGenerator::create_plan(InsertLogicalNode &insert_oper, unique_ptr<PhysicalOperator> &oper)
{
  Table *table = insert_oper.table();
  vector<vector<Value>> multi_values;
  for (int i = 0; i < insert_oper.multi_values().size(); i++) {
    vector<Value> &values = insert_oper.values(i);
    multi_values.push_back(values);
  }
  InsertPhysicalOperator *insert_phy_oper = new InsertPhysicalOperator(table, std::move(multi_values));
  oper.reset(insert_phy_oper);
  return RC::SUCCESS;
}

RC PhysicalOperatorGenerator::create_plan(DeleteLogicalNode &delete_oper, unique_ptr<PhysicalOperator> &oper)
{
  vector<unique_ptr<LogicalNode>> &child_opers = delete_oper.children();

  unique_ptr<PhysicalOperator> child_physical_oper;

  RC rc = RC::SUCCESS;
  if (!child_opers.empty()) {
    LogicalNode *child_oper = child_opers.front().get();
    rc = create(*child_oper, child_physical_oper, true);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to create physical operator. rc=%s", strrc(rc));
      return rc;
    }
  }

  oper = unique_ptr<PhysicalOperator>(new DeletePhysicalOperator(delete_oper.table()));
  oper->isdelete_ = true;
  if (child_physical_oper) {
    oper->add_child(std::move(child_physical_oper));
  }
  return rc;
}

RC PhysicalOperatorGenerator::create_plan(UpdateLogicalNode &update_oper, unique_ptr<PhysicalOperator> &oper)
{
  vector<unique_ptr<LogicalNode>> &child_opers = update_oper.children();

  unique_ptr<PhysicalOperator> child_physical_oper;

  RC rc = RC::SUCCESS;
  if (!child_opers.empty()) {
    LogicalNode *child_oper = child_opers.front().get();
    rc = create(*child_oper, child_physical_oper);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to create physical operator. rc=%s", strrc(rc));
      return rc;
    }
  }

  // 将 update_units 从逻辑算子转移给物理算子，避免重复释放
  oper = unique_ptr<PhysicalOperator>(new UpdatePhysicalOperator(update_oper.table(), std::move(update_oper.update_units())));

  if (child_physical_oper) {
    oper->add_child(std::move(child_physical_oper));
  }
  return rc;
}

RC PhysicalOperatorGenerator::create_plan(
    ExplainLogicalNode &explain_oper, unique_ptr<PhysicalOperator> &oper, bool is_delete)
{
  vector<unique_ptr<LogicalNode>> &child_opers = explain_oper.children();

  RC rc = RC::SUCCESS;
  unique_ptr<PhysicalOperator> explain_physical_oper(new ExplainPhysicalOperator);
  for (unique_ptr<LogicalNode> &child_oper : child_opers) {
    unique_ptr<PhysicalOperator> child_physical_oper;
    rc = create(*child_oper, child_physical_oper, is_delete);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to create child physical operator. rc=%s", strrc(rc));
      return rc;
    }

    explain_physical_oper->add_child(std::move(child_physical_oper));
  }

  oper = std::move(explain_physical_oper);
  oper->isdelete_ = is_delete;
  return rc;
}

// TODO [Lab3] 根据LogicalNode生成对应的PhyiscalOperator
RC PhysicalOperatorGenerator::create_plan(
    JoinLogicalNode &join_oper, unique_ptr<PhysicalOperator> &oper)
{
  // 孩子节点
  vector<unique_ptr<LogicalNode>> &child_opers = join_oper.children();
  ASSERT(child_opers.size() == 2, "Join logical operator's sub oper number should be 2");
  // 左子节点
  unique_ptr<PhysicalOperator> left_oper;
  RC rc = create(*child_opers[0], left_oper);
  if (rc != RC::SUCCESS) {
    LOG_WARN("Failed to create left child operator of join operator. rc=%s", strrc(rc));
    return rc;
  }

  unique_ptr<PhysicalOperator> right_oper;
  rc = create(*child_opers[1], right_oper);
  if (rc != RC::SUCCESS) {
    LOG_WARN("Failed to create right child operator of join operator. rc=%s", strrc(rc));
    return rc;
  }

  auto *join_operator = new JoinPhysicalOperator();

  join_operator->set_condition(std::move(join_oper.condition()));

  // 等值连接时启用哈希连接
  if (join_oper.condition() != nullptr) {
    if (join_oper.condition()->type() == ExprType::COMPARISON) {
      auto *comp_expr = dynamic_cast<ComparisonExpr*>(join_oper.condition().get());
      if (comp_expr != nullptr && comp_expr->comp() == EQUAL_TO) {
        join_operator->enable_hash_join();
        LOG_TRACE("Hash join enabled for equi-join operation");
      }
    }
  }

  join_operator->add_child(std::move(left_oper));
  join_operator->add_child(std::move(right_oper));
  
  oper = unique_ptr<PhysicalOperator>(join_operator);

  return RC::SUCCESS;
}