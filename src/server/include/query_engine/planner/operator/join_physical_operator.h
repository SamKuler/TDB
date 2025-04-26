#pragma once

#include "physical_operator.h"
#include "include/query_engine/structor/tuple/join_tuple.h"

// TODO [Lab3] join算子的头文件定义，根据需要添加对应的变量和方法
class JoinPhysicalOperator : public PhysicalOperator
{
public:
  JoinPhysicalOperator();
  ~JoinPhysicalOperator() override = default;

  PhysicalOperatorType type() const override
  {
    return PhysicalOperatorType::JOIN;
  }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;
  Tuple *current_tuple() override;

  void set_condition(std::unique_ptr<Expression> &&condition)
  {
    condition_ = std::move(condition);
  }

  void enable_hash_join()
  {
    use_hash_join_ = true;
  }
private:
  RC get_join_keys();
  RC build_hash_table();
  RC get_value_from_tuple(Tuple* tuple, const std::string& table_name, const std::string& field_name, Value& value);

private:
  Trx *trx_ = nullptr;
  JoinedTuple joined_tuple_;  // 当前关联的左右两个tuple
  std::unique_ptr<Expression> condition_;  // 连接条件
  
  bool started_ = false;  // 是否开始执行
  bool left_ready_ = false;  // 左表是否已经遍历完

  // Hash Join
  bool use_hash_join_ = false;          // 是否使用哈希连接
  bool hash_table_built_ = false;       // 哈希表是否已建立
  std::string left_key_field_;          // 左表连接键字段
  std::string right_key_field_;         // 右表连接键字段
  std::string left_table_name_;          // 左表名称
  std::string right_table_name_;         // 右表名称

  // Hash: 键是连接键值，值是匹配该键的左表元组集合
  std::unordered_map<std::string, std::vector<Tuple*>> hash_table_;

  // 当前处理状态
  Tuple* current_right_tuple_ = nullptr;                // 当前右表元组
  std::vector<Tuple*>::iterator current_matches_iter_;  // 当前匹配集合迭代器
  std::vector<Tuple*>::iterator current_matches_end_;   // 当前匹配集合结束位置
  bool has_current_matches_ = false;                   // 是否有当前匹配

};
