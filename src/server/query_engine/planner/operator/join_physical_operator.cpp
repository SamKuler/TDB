#include "include/query_engine/planner/operator/join_physical_operator.h"
#include "include/query_engine/structor/expression/comparison_expression.h"

/* TODO [Lab3] join的算子实现，需要根据join_condition实现Join的具体逻辑，
  最后将结果传递给JoinTuple, 并由current_tuple向上返回
 JoinOperator通常会遵循下面的被调用逻辑：
 operator.open()
 while(operator.next()){
    Tuple *tuple = operator.current_tuple();
 }
 operator.close()
*/

JoinPhysicalOperator::JoinPhysicalOperator() = default;

RC JoinPhysicalOperator::get_join_keys()
{
  // 只处理简单的等值连接条件
  if (condition_ == nullptr || condition_->type() != ExprType::COMPARISON) {
    LOG_WARN("Hash join requires a comparison condition");
    return RC::INVALID_ARGUMENT;
  }
  
  auto* comp_expr = dynamic_cast<ComparisonExpr*>(condition_.get());
  if (comp_expr == nullptr || comp_expr->comp() != EQUAL_TO) {
    LOG_WARN("Hash join requires an equality comparison");
    return RC::INVALID_ARGUMENT;
  }
  
  // 字段表达式
  Expression* left_side = comp_expr->left().get();
  Expression* right_side = comp_expr->right().get();
  
  if (left_side->type() != ExprType::FIELD || right_side->type() != ExprType::FIELD) {
    LOG_WARN("Hash join requires field expressions on both sides");
    return RC::INVALID_ARGUMENT;
  }
  
  auto* left_field = dynamic_cast<FieldExpr*>(left_side);
  auto* right_field = dynamic_cast<FieldExpr*>(right_side);
  

  left_table_name_ = left_field->table_name();
  right_table_name_ = right_field->table_name();
  left_key_field_ = left_field->field_name();
  right_key_field_ = right_field->field_name();
  
  return RC::SUCCESS;
}

// 从元组中获取字段值
RC JoinPhysicalOperator::get_value_from_tuple(Tuple* tuple, const std::string& table_name, const std::string& field_name, Value& value)
{
  TupleCellSpec spec(table_name.c_str(), field_name.c_str());
  // 查找字段
  RC rc = tuple->find_cell(spec, value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("Field %s.%s not found in tuple", table_name.c_str(), field_name.c_str());
  }
  return rc;
}

// 构建哈希表
RC JoinPhysicalOperator::build_hash_table()
{
  if (hash_table_built_) {
    return RC::SUCCESS;
  }
  
  // 提取连接键
  RC rc = get_join_keys();
  if (rc != RC::SUCCESS) {
    LOG_WARN("Failed to extract join keys");
    return rc;
  }
  
  PhysicalOperator *left_child = children_[0].get();
  
  // 从左表构建哈希表
  while ((rc = left_child->next()) == RC::SUCCESS) {
    Tuple* left_tuple = left_child->current_tuple();
    
    // 获取连接键值
    Value key_value;
    rc = get_value_from_tuple(left_tuple,left_table_name_, left_key_field_, key_value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("Failed to get join key value from left tuple");
      return rc;
    }
    
    // 字符串表示作为哈希表键
    std::string key_str = key_value.to_string();
    hash_table_[key_str].push_back(left_tuple);
  }
  
  // 正常EOF检测
  if (rc != RC::RECORD_EOF) {
    LOG_WARN("Error occurred while reading left table: %s", strrc(rc));
    return rc;
  }
  
  hash_table_built_ = true;
  return RC::SUCCESS;
}

// 执行next()前的准备工作, trx是之后事务中会使用到的，这里不用考虑
RC JoinPhysicalOperator::open(Trx *trx)
{
  trx_ = trx;
  RC rc = children_[0]->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("Failed to open left child of join operator. rc=%s", strrc(rc));
    return rc;
  }
  rc = children_[1]->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("Failed to open right child of join operator. rc=%s", strrc(rc));
    children_[0]->close();
    return rc;
  }

  left_ready_ = false;
  started_ = false;
  hash_table_built_ = false;
  has_current_matches_ = false;
  
  // 如果启用哈希连接且有连接条件，构建哈希表
  if (use_hash_join_ && condition_ != nullptr) {
    rc = build_hash_table();
    if (rc != RC::SUCCESS) {
      LOG_WARN("Failed to build hash table. rc=%s", strrc(rc));
      children_[1]->close();
      children_[0]->close();
      return rc;
    }
    
    // 重置左表
    children_[0]->close();
    rc = children_[0]->open(trx);
    if (rc != RC::SUCCESS) {
      LOG_WARN("Failed to reopen left child after building hash table. rc=%s", strrc(rc));
      return rc;
    }
  }
  
  
  return RC::SUCCESS;
}

// 计算出接下来需要输出的数据，并将结果set到join_tuple中
// 如果没有更多数据，返回RC::RECORD_EOF
RC JoinPhysicalOperator::next()
{
  if (use_hash_join_ && hash_table_built_) {
    // Hash Join
    
    // 如果当前右表元组有更多匹配的左表元组
    if (has_current_matches_) {
      ++current_matches_iter_;
      if (current_matches_iter_ != current_matches_end_) {
        // 返回下一个匹配
        joined_tuple_.set_left(*current_matches_iter_);
        joined_tuple_.set_right(current_right_tuple_);
        return RC::SUCCESS;
      }
      has_current_matches_ = false;
    }
    
    // 获取下一个右表元组
    PhysicalOperator *right_child = children_[1].get();
    RC rc;
    
    while ((rc = right_child->next()) == RC::SUCCESS) {
      current_right_tuple_ = right_child->current_tuple();
      
      // 右键值
      Value key_value;
      rc = get_value_from_tuple(current_right_tuple_, right_table_name_, right_key_field_, key_value);
      if (rc != RC::SUCCESS) {
        LOG_WARN("Failed to get join key value from right tuple");
        return rc;
      }
      
      // 查找
      std::string key_str = key_value.to_string();
      auto it = hash_table_.find(key_str);
      
      if (it != hash_table_.end() && !it->second.empty()) {
        // 找到匹配
        current_matches_iter_ = it->second.begin();
        current_matches_end_ = it->second.end();
        has_current_matches_ = true;
        
        // 返回第一个匹配
        joined_tuple_.set_left(*current_matches_iter_);
        joined_tuple_.set_right(current_right_tuple_);
        return RC::SUCCESS;
      }
    }    
  }
  else {
    // Nested Loop Join
    PhysicalOperator *left_child = children_[0].get();
    PhysicalOperator *right_child = children_[1].get();
    if (!started_) {
      started_ = true;
      // 先从左表中取出第一个tuple
      RC rc = left_child->next();
      if (rc != RC::SUCCESS) {
        LOG_TRACE("No tuples in left child of join operator. rc=%s", strrc(rc));
        return rc;
      }
      left_ready_ = true;
    }
    if (!left_ready_) {
      return RC::RECORD_EOF;
    }

    while (left_ready_) {
      // 先从右表中取出第一个tuple
      RC rc = right_child->next();
      if (rc == RC::SUCCESS) {
        // 右表中有数据
        joined_tuple_.set_left(left_child->current_tuple());
        joined_tuple_.set_right(right_child->current_tuple());

        // 没有连接条件
        if (condition_ == nullptr) {
          return RC::SUCCESS;
        }

        // 有连接条件
        Value condition_val;
        rc = condition_->get_value(joined_tuple_,condition_val);
        if (rc != RC::SUCCESS) {
          LOG_WARN("Failed to evaluate join condition. rc=%s", strrc(rc));
          return rc;
        }
        if(condition_val.get_boolean()){
          //满足，返回结果
          return RC::SUCCESS;
        }
        continue;
      }
      // 右表中没有数据，从左表中取出下一个tuple
      rc= left_child->next();
      if(rc!= RC::SUCCESS){
        // 左表中也没有数据
        left_ready_ = false;
        return rc;
      }
      // 左表中有数据，重置右表
      rc = right_child->close();
      rc = right_child->open(trx_);
      if (rc != RC::SUCCESS) {
        LOG_WARN("Failed to open right child of join operator. rc=%s", strrc(rc));
        return rc;
      }  
    }
  }
  return RC::RECORD_EOF;
}

// 节点执行完成，清理左右子算子
RC JoinPhysicalOperator::close()
{
  RC rc = RC::SUCCESS;

  if(children_.size()>1){
    RC rc_right= children_[1]->close();
    if (rc_right != RC::SUCCESS) {
      LOG_WARN("Failed to close right child of join operator. rc=%s", strrc(rc_right));
      rc = rc_right;
    }
  }
  if(!children_.empty()){
    RC rc_left= children_[0]->close();
    if (rc_left != RC::SUCCESS) {
      LOG_WARN("Failed to close left child of join operator. rc=%s", strrc(rc_left));
      rc = rc_left;
    }
  }
  // if (condition_ != nullptr) {
  //   condition_.reset();
  // }
  // 清理哈希表
  hash_table_.clear();
  hash_table_built_ = false;
  has_current_matches_ = false;
  return rc;
}

Tuple *JoinPhysicalOperator::current_tuple()
{
  return &joined_tuple_;
}
