# 代码审查报告

## 基本信息

**Commit ID**: `<commit_id>`  
**提交时间**: `<commit_date>`  
**作者**: `<author>`  
**提交消息**: `<commit_message>`

---

## 审查结论

**审查结果**: `<review_result>` (通过/有条件通过/不通过)

**综合评分**: `<overall_score>/10`

| 维度 | 评分 | 说明 |
|------|------|------|
| 正确性 | `<score>/10` | `<comment>` |
| 安全性 | `<score>/10` | `<comment>` |
| 可维护性 | `<score>/10` | `<comment>` |
| 性能 | `<score>/10` | `<comment>` |
| 代码风格 | `<score>/10` | `<comment>` |

**审查意见**: `<review_comment>`

---

## 问题汇总

> 以下是本次审查发现的所有问题，按严重程度排序。如需了解详细分析过程，请查看后续章节。

### 严重问题 (必须修复)

```
<问题分类>
<问题描述>
[问题代码文件名：代码行号]
```

### 中等问题 (建议修复)

```
<问题分类>
<问题描述>
[问题代码文件名：代码行号]
```

### 轻微问题 (可选修复)

```
<问题分类>
<问题描述>
[问题代码文件名：代码行号]
```

### 问题统计

| 问题类型 | 数量 |
|----------|------|
| 接口变更未同步 | `<count>` |
| 未解决问题 | `<count>` |
| 资源泄露 | `<count>` |
| 空指针风险 | `<count>` |
| 死循环风险 | `<count>` |
| 性能问题 | `<count>` |
| 非最优解 | `<count>` |
| 安全漏洞 | `<count>` |
| 代码风格不一致 | `<count>` |
| **总计** | **`<total>`** |

---

# 详细分析

> 以下是详细的分析过程，如果已在问题汇总中了解所有问题，可跳过此部分。

---

## 1. 变更摘要

### 修改文件列表
- `<file_path_1>` (新增/修改/删除)
- `<file_path_2>` (新增/修改/删除)
- ...

### 变更统计
- 新增行数: `<added_lines>`
- 删除行数: `<deleted_lines>`
- 净变更: `<net_change>`

---

## 2. 代码变更分析

### 2.1 涉及的函数/方法

#### 新增函数
- `<function_name>` - `<file_path>:<line_number>`
  - 功能描述: `<description>`
  - 参数: `<parameters>`
  - 返回值: `<return_type>`

#### 修改函数
- `<function_name>` - `<file_path>:<line_number>`
  - 修改类型: `<modification_type>` (签名变更/逻辑变更/重构)
  - 变更说明: `<change_description>`

#### 删除函数
- `<function_name>` - `<file_path>:<line_number>`
  - 删除原因: `<reason>`

### 2.2 涉及的类

#### 新增类
- `<class_name>` - `<file_path>:<line_number>`
  - 功能描述: `<description>`

#### 修改类
- `<class_name>` - `<file_path>:<line_number>`
  - 修改内容: `<modification_details>`

#### 删除类
- `<class_name>` - `<file_path>:<line_number>`
  - 删除原因: `<reason>`

### 2.3 接口变更

#### API 端点变更
- `<endpoint>` - `<file_path>:<line_number>`
  - 变更类型: `<change_type>` (URL/请求格式/响应格式)
  - 变更详情: `<details>`

#### 函数签名变更
- `<function_name>` - `<file_path>:<line_number>`
  - 原签名: `<old_signature>`
  - 新签名: `<new_signature>`
  - 影响范围: `<impact_scope>`

---

## 3. 调用链分析

> 分析深度: `<depth>` 层

### 3.1 函数调用关系图

```
<function_name>
├── 定义位置: <file_path>:<line_number>
├── 被以下函数调用（上游）:
│   ├── <caller_1> (<file_path>:<line_number>)
│   ├── <caller_2> (<file_path>:<line_number>)
│   └── ...
└── 调用以下函数（下游）:
    ├── <callee_1> (<file_path>:<line_number>)
    ├── <callee_2> (<file_path>:<line_number>)
    └── ...
```

### 3.2 调用链文档

#### `<function_name>` 的完整调用链

**上游调用链** (谁调用了这个函数):
1. `<caller_1>` → `<caller_2>` → ... → `<function_name>`
   - 调用位置: `<file_path>:<line_number>`

**下游调用链** (这个函数调用了谁):
1. `<function_name>` → `<callee_1>` → `<callee_2>` → ...
   - 调用位置: `<file_path>:<line_number>`

---

## 4. 接口变更评估

### 4.1 接口变更检测

**检测结果**: `<has_interface_changes>` (是/否)

如果检测到接口变更:

#### 变更详情
- **变更类型**: `<change_type>`
- **变更位置**: `<file_path>:<line_number>`
- **变更描述**: `<description>`

#### 影响范围分析
- **受影响的调用点总数**: `<total_call_sites>`
- **已更新的调用点**: `<updated_call_sites>`
- **可能遗漏的调用点**: `<missed_call_sites>`

#### 遗漏的调用点列表
- `<file_path>:<line_number>` - `<function_name>`
- `<file_path>:<line_number>` - `<function_name>`
- ...

#### 向后兼容性评估
- **兼容性**: `<compatibility>` (完全兼容/部分兼容/不兼容)
- **风险评估**: `<risk_assessment>`
- **建议**: `<recommendations>`

---

## 5. 问题解决验证

### 5.1 提交描述分析

**提交描述中的问题**:
- `<problem_1>`
- `<problem_2>`
- ...

### 5.2 解决方案验证

#### 问题 1: `<problem_1>`
- **解决状态**: `<status>` (已解决/部分解决/未解决)
- **解决方案**: `<solution>`
- **验证结果**: `<verification>`
- **评价**: `<evaluation>`

#### 问题 2: `<problem_2>`
- **解决状态**: `<status>`
- **解决方案**: `<solution>`
- **验证结果**: `<verification>`
- **评价**: `<evaluation>`

### 5.3 整体评价
- **问题解决完整性**: `<completeness>`
- **解决方案质量**: `<quality>`

---

## 6. 潜在问题检测详情

### 6.1 资源泄露

**检测结果**: `<has_resource_leaks>` (是/否)

**检测项**:
- 文件句柄是否正确关闭
- 数据库连接是否正确释放
- 网络连接是否正确关闭
- 内存分配是否匹配释放
- 上下文管理器（with 语句）是否正确使用

### 6.2 空指针/空引用风险

**检测结果**: `<has_null_pointer_risks>` (是/否)

**检测项**:
- 变量在使用前是否进行了空值检查
- 字典/对象属性访问前是否检查键/属性存在
- 数组/列表访问前是否检查索引边界
- 函数返回值是否可能为 None/null

### 6.3 死循环风险

**检测结果**: `<has_infinite_loop_risks>` (是/否)

**检测项**:
- 循环条件是否可能永远为真
- 循环变量是否正确更新
- 递归函数是否有正确的终止条件
- 是否有无限递归的风险

### 6.4 性能问题

**检测结果**: `<has_performance_issues>` (是/否)

**检测项**:
- 算法复杂度分析：时间复杂度、空间复杂度是否合理
- 系统运行效率：是否会影响系统整体性能
- 调用频率合理性：功能被调用的频率是否合理
  - 是否在高频调用的函数中（如渲染帧中的 update 函数）调用了开销较大的函数
  - 是否在循环中进行了昂贵的操作
  - 是否有不必要的重复计算
- 数据库查询优化：是否存在 N+1 查询问题
- 资源使用：是否有内存泄漏、缓存未命中等问题

### 6.5 非最优解

**检测结果**: `<has_suboptimal_solutions>` (是/否)

**检测项**:
- 是否有更简洁的实现方式
- 是否有更高效的算法或数据结构
- 是否可以利用语言特性简化代码
- 是否遵循了最佳实践

---

## 7. 安全问题检测详情

### 7.1 SQL 注入风险

**检测结果**: `<has_sql_injection_risks>` (是/否)

**检测项**:
- 是否使用参数化查询
- 是否有字符串拼接 SQL
- 用户输入是否经过验证

### 7.2 XSS 漏洞

**检测结果**: `<has_xss_vulnerabilities>` (是/否)

**检测项**:
- 用户输入是否经过转义后才输出
- 是否使用了安全的模板引擎
- 是否有 innerHTML 或类似操作

### 7.3 硬编码敏感信息

**检测结果**: `<has_hardcoded_secrets>` (是/否)

**检测项**:
- 是否硬编码了密码、API Key、Token
- 是否有敏感信息写入日志
- 配置文件中是否包含明文密码

### 7.4 权限检查缺失

**检测结果**: `<has_missing_auth_checks>` (是/否)

**检测项**:
- API 端点是否有适当的权限验证
- 敏感操作前是否验证用户身份
- 是否有越权访问风险

### 7.5 输入验证不足

**检测结果**: `<has_input_validation_issues>` (是/否)

**检测项**:
- 用户输入是否经过验证
- 是否有类型检查和边界检查
- 文件上传是否有类型和大小限制

---

## 8. 代码风格检查详情

### 8.1 风格一致性检查

**检测结果**: `<has_style_issues>` (是/否)

### 8.2 具体问题列表

#### 命名规范
- `<file_path>:<line_number>` - `<issue_description>`

#### 格式化问题
- `<file_path>:<line_number>` - `<issue_description>`

#### 注释风格
- `<file_path>:<line_number>` - `<issue_description>`

#### 导入语句
- `<file_path>:<line_number>` - `<issue_description>`

---

## 9. 主要优点

1. `<advantage_1>`
2. `<advantage_2>`
3. ...

---

## 附录

### A. 审查配置

| 配置项 | 值 |
|--------|-----|
| 调用链深度 | `<depth>` |
| 跳过风格检查 | `<skip_style>` |
| 跳过安全检测 | `<skip_security>` |

---

**报告生成时间**: `<report_generation_time>`  
**审查工具版本**: Code Review Skill v1.2
