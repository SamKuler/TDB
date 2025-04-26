#include "include/query_engine/planner/operator/join_physical_operator.h"

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
  
  return RC::SUCCESS;
}

// 计算出接下来需要输出的数据，并将结果set到join_tuple中
// 如果没有更多数据，返回RC::RECORD_EOF
RC JoinPhysicalOperator::next()
{
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

  while (true) {
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
  return rc;
}

Tuple *JoinPhysicalOperator::current_tuple()
{
  return &joined_tuple_;
}
