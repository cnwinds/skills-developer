---
name: code-review
description: 对 Git commit 或当前工作区未提交代码进行全面的代码审查，包括获取变更信息和 diff、分析代码变更、生成调用链文档、评估接口变更、验证问题解决、检测潜在问题（资源泄露、空指针、死循环、性能问题、安全漏洞等）、检查代码风格。使用场景：当用户提供 commit_id 需审查某次提交时，或未提供 commit_id 时审查当前工作区未提交的变更（已暂存 + 未暂存），或需要评估代码质量、安全性和可维护性时。
---

# Code Review Skill

对 Git commit 或当前工作区未提交代码进行全面的代码审查，生成详细的审查报告。

## How It Works

1. **输入判断**：若用户未提供 commit_id，则审查**当前工作区未提交的变更**（已暂存 + 未暂存）；若提供了 commit_id，则校验格式与存在性，检测 merge commit 并询问用户。
2. **获取变更信息**：无 commit_id 时用 `git diff` / `git diff --cached` 获取工作区 diff；有 commit_id 时用 git 获取该提交信息与 diff，先检查变更量再决定是否拉取完整 diff。
3. **代码变更分析**：识别新增/修改/删除的函数、类、接口。
4. **调用链分析**：按配置深度分析被修改函数的上下游调用关系。
5. **接口变更评估**：检查接口变更、使用点是否同步、向后兼容性。
6. **问题解决验证**：核对代码修改是否真正解决 commit 描述中的问题。
7. **潜在问题检测**：资源泄露、空指针、死循环、性能、非最优解。
8. **安全问题检测**：SQL 注入、XSS、硬编码敏感信息、权限与输入验证。
9. **代码风格检查**：与项目现有风格对比。
10. **生成审查报告**：按 `assets/report-template.md` 输出，可选保存到 `review/YYYYMMDD-<commit_id>.md`。

## Usage

本 skill 为指令型，由 Agent 阅读 SKILL.md 后执行。用户通过自然语言指定 commit_id（可选）与可选参数。

**审查未提交代码**（不提供 commit_id）：`请审查当前未提交的代码` / `审查一下我改动的代码`

**审查指定提交**：`请审查 commit <commit_id>`

**带参数**：`请审查当前改动，调用链深度 3 层` / `请审查 commit <commit_id>，保存报告到 ./reviews 目录`

**Arguments:** 无（参数由自然语言解析）

## 角色设定

你现在是一位拥有 10 年以上经验的资深代码审查专家，精通代码审查、架构设计和代码质量评估。你具备深入理解代码逻辑、识别潜在问题、评估代码变更影响的能力。

## 能力概述

本 Skill 提供以下代码审查能力：

| 能力 | 说明 |
|------|------|
| 变更信息获取 | 有 commit_id 时获取提交描述与 diff；无 commit_id 时获取工作区未提交 diff（已暂存 + 未暂存） |
| 代码变更分析 | 识别新增/修改/删除的函数、类、接口 |
| 调用链分析 | 分析被修改函数的上下游调用关系（默认 2 层深度） |
| 接口变更评估 | 检查接口变更、同步情况和向后兼容性 |
| 问题解决验证 | 验证代码修改是否真正解决了 commit 描述中的问题 |
| 潜在问题检测 | 资源泄露、空指针、死循环、性能问题、非最优解 |
| 安全问题检测 | SQL 注入、XSS、硬编码敏感信息、权限检查缺失 |
| 代码风格检查 | 与项目现有代码风格对比 |

## 输入参数

| 参数 | 必填 | 默认值 | 说明 |
|------|------|--------|------|
| commit_id | 否 | - | 需要审查的 Git commit ID；不提供时审查当前工作区未提交的代码 |
| depth | 否 | 2 | 调用链分析深度（1-5 层） |
| save_report | 否 | true | 是否保存报告文件（默认保存） |
| verbose | 否 | false | 是否输出详细分析过程 |
| skip_style | 否 | false | 是否跳过代码风格检查 |
| skip_security | 否 | false | 是否跳过安全检测 |

---

## 执行步骤

### 0. 输入判断与验证

在开始审查前，先判断用户是否提供了 commit_id：

**未提供 commit_id**：审查**当前工作区未提交的代码**（已暂存 + 未暂存），跳过下方 commit 验证，直接进入「1. 获取变更信息」中的「无 commit_id 分支」。

**提供了 commit_id**：按以下步骤验证后，进入「1. 获取变更信息」中的「有 commit_id 分支」。

**验证 commit_id 格式**:
```bash
# commit_id 应为 7-40 位十六进制字符
git cat-file -t <commit_id>
```

**异常处理**:
| 情况 | git 返回 | 处理方式 |
|------|----------|----------|
| 有效 commit | "commit" | 继续执行 |
| 无效格式 | error: ... | 提示"commit ID 格式无效" |
| 不存在 | fatal: Not a valid object name | 提示"无法找到 commit" |
| 非 git 仓库 | fatal: not a git repository | 提示"当前目录不是 Git 仓库" |

**检测 Merge Commit**（仅在有 commit_id 时）:
```bash
# Windows PowerShell
(git cat-file -p <commit_id> | Select-String "^parent").Count

# Linux/Mac (如果使用bash)
git cat-file -p <commit_id> | grep "^parent" | wc -l
```
- 返回 > 1：这是 merge commit，询问用户处理方式
- 返回 = 1：普通 commit，继续执行

---

### 1. 获取变更信息

**使用工具**: Shell

---

#### 分支 A：无 commit_id（审查当前工作区未提交代码）

**步骤 1: 确认在 Git 仓库内**
```bash
git rev-parse --is-inside-work-tree
```
- 若返回非 `true`，提示「当前目录不是 Git 仓库」。

**步骤 2: 获取未提交变更统计**
```bash
# 未暂存变更统计
git diff --stat

# 已暂存变更统计
git diff --cached --stat
```
解析两段输出的最后一行（总行数统计），相加得到总变更量。

**步骤 3: 检查变更量**
- 解析上述 `--stat` 输出的最后一行，得到总增删行数。
- 若 `新增行数 + 删除行数 > 1000`，询问用户是否继续完整分析；若跳过，仅分析文件列表与函数签名。

**步骤 4: 获取完整 diff（需要时）**
```bash
# 未暂存的变更
git diff

# 已暂存的变更
git diff --cached
```
审查时**两者都要纳入**：先理解 `git diff --cached`（即将提交的），再理解 `git diff`（工作区未暂存），合并为「当前未提交的全部变更」进行分析。

**提取信息**（无 commit 元数据时）:
- 说明来源：「当前工作区未提交的变更（已暂存 + 未暂存）」。
- 修改的文件列表（从 `git diff --stat` 与 `git diff --cached --stat` 汇总）。
- 完整代码变更：`git diff` 与 `git diff --cached` 的输出。

---

#### 分支 B：有 commit_id（审查指定提交）

**步骤 1: 获取基本信息和变更统计**
```bash
# 获取 commit 基本信息（不包含diff）
git log -1 <commit_id> --format="%H%n%an%n%ae%n%ad%n%s%n%b"

# 获取变更统计（仅统计信息，不包含diff内容）
git diff --stat <commit_id>~1 <commit_id>
```

**步骤 2: 检查变更量**
```bash
# Windows PowerShell
git diff --stat <commit_id>~1 <commit_id> | Select-Object -Last 1

# Linux/Mac (如果使用bash)
git diff --stat <commit_id>~1 <commit_id> | tail -1
```

**变更量判断**:
- 解析统计信息的最后一行，提取新增和删除的行数
- 如果 `新增行数 + 删除行数 > 1000`，询问用户是否继续完整分析
- 如果用户选择跳过，只分析文件列表和函数签名，不获取完整diff

**步骤 3: 获取完整 diff（仅在需要时）**
```bash
# 只有在变更量合理或用户确认后才获取完整diff
git diff <commit_id>~1 <commit_id>

# 或者使用 git show（包含commit信息）
git show <commit_id> --no-patch  # 仅获取commit信息，不包含diff
git diff <commit_id>~1 <commit_id>  # 单独获取diff
```

**提取信息**:
- 提交消息（commit message）
- 作者信息（姓名、邮箱）
- 提交时间
- 修改的文件列表（从 `--stat` 获取）
- 完整的代码变更（diff，仅在需要时获取）

---

**通用注意事项**:
- ⚠️ **避免一次性获取大型diff**：对于包含大量文件或大文件的变更，先检查变更量再决定是否获取完整diff
- ⚠️ **跨平台兼容**：Windows使用PowerShell命令，Linux/Mac使用bash命令
- ⚠️ **性能优化**：对于大型变更（>1000行），可以只分析关键文件，跳过二进制文件或自动生成的文件

---

### 2. 代码变更分析

**使用工具**: Read、Grep

**分析内容**:
- 识别所有被修改、新增或删除的函数/方法
- 识别所有被修改、新增或删除的类
- 识别所有被修改的接口定义（API endpoints、函数签名等）
- 分析每个变更的上下文和影响范围

**工具使用**:
```
# 读取修改的文件
Read: <file_path>

# 搜索函数定义（Python 示例）
Grep: pattern="def \w+\(" path="<file_path>"

# 搜索类定义
Grep: pattern="class \w+" path="<file_path>"
```

---

### 3. 调用链分析

**使用工具**: Grep、SemanticSearch

**分析深度**: 默认 2 层，可通过 depth 参数配置（1-5 层）

**对于每个被修改的函数**:

1. **查找调用者（上游）**:
```
# 使用 Grep 搜索函数调用
Grep: pattern="<function_name>\s*\(" type="py"

# 使用语义搜索查找调用点
SemanticSearch: query="哪里调用了 <function_name> 函数？"
```

2. **查找被调用者（下游）**:
```
# 读取函数体，分析其内部调用
Read: <file_path> offset=<start_line> limit=<function_length>
```

3. **生成调用关系图**:
```
<function_name>
├── 定义位置: <file_path>:<line_number>
├── 被以下函数调用（上游）:
│   ├── <caller_1> (<file_path>:<line_number>)
│   └── <caller_2> (<file_path>:<line_number>)
└── 调用以下函数（下游）:
    ├── <callee_1> (<file_path>:<line_number>)
    └── <callee_2> (<file_path>:<line_number>)
```

**深度限制说明**:
- 深度 1：只分析直接调用者和被调用者
- 深度 2：分析两层调用关系（推荐）
- 深度 3+：分析更深层次（大型重构时使用）

---

### 4. 接口变更评估

**使用工具**: Grep、SemanticSearch

**检查内容**:
- 函数签名是否改变（参数、返回值类型）
- 函数行为是否改变（可能影响调用者）
- API 端点是否改变（URL、请求/响应格式）
- 数据库模型是否改变（表结构、字段）

**如果接口发生改变**:

1. **搜索所有使用点**:
```
# 搜索函数调用
Grep: pattern="<function_name>\(" output_mode="files_with_matches"

# 语义搜索
SemanticSearch: query="哪些地方使用了 <function_name>？"
```

2. **检查每个使用点是否已更新**
3. **识别可能遗漏的调用点**
4. **评估向后兼容性**

---

### 5. 问题解决验证

**使用工具**: Read

- **有 commit_id 时**：  
  1. 仔细阅读提交描述（commit message）  
  2. 分析提交描述中要解决的问题  
  3. 检查代码修改是否真正解决了描述中的问题：修改是否针对问题根源、是否完整、是否有更好方案  

- **无 commit_id 时**：无提交描述可对照，本步骤可省略；若用户在前文说明了修改目的，可据此简要验证变更是否贴合目的。

---

### 6. 潜在问题检测

**使用工具**: Read

对代码变更进行深度分析，检查以下潜在问题：

#### a) 资源泄露
- 文件句柄是否正确关闭
- 数据库连接是否正确释放
- 网络连接是否正确关闭
- 内存分配是否匹配释放
- 上下文管理器（with 语句）是否正确使用

#### b) 空指针/空引用风险
- 变量在使用前是否进行了空值检查
- 字典/对象属性访问前是否检查键/属性存在
- 数组/列表访问前是否检查索引边界
- 函数返回值是否可能为 None/null

#### c) 死循环风险
- 循环条件是否可能永远为真
- 循环变量是否正确更新
- 递归函数是否有正确的终止条件
- 是否有无限递归的风险

#### d) 性能问题
- **算法复杂度**: 时间复杂度、空间复杂度是否合理
- **调用频率合理性**:
  - 是否在高频调用的函数中（如渲染帧中的 update 函数）调用了开销较大的函数
  - 是否在循环中进行了昂贵的操作
  - 是否有不必要的重复计算
- **数据库查询**: 是否存在 N+1 查询问题
- **资源使用**: 是否有内存泄漏、缓存未命中等问题

#### e) 非最优解
- 是否有更简洁的实现方式
- 是否有更高效的算法或数据结构
- 是否可以利用语言特性简化代码
- 是否遵循了最佳实践

---

### 7. 安全问题检测

**使用工具**: Grep、Read

如果 skip_security=false（默认），执行以下安全检查：

#### a) SQL 注入风险
```
Grep: pattern="execute\s*\(.*%|execute\s*\(.*\+|execute\s*\(.*\.format"
```
- 检查是否使用参数化查询
- 检查是否有字符串拼接 SQL

#### b) XSS 漏洞
- 检查用户输入是否经过转义后才输出
- 检查是否使用了安全的模板引擎

#### c) 硬编码敏感信息
```
Grep: pattern="(password|secret|api_key|token)\s*=\s*['\"][^'\"]+['\"]" -i
```
- 检查是否硬编码了密码、API Key、Token
- 检查是否有敏感信息写入日志

#### d) 权限检查缺失
- 检查 API 端点是否有适当的权限验证
- 检查敏感操作前是否验证用户身份

#### e) 输入验证不足
- 检查用户输入是否经过验证
- 检查是否有类型检查和边界检查

---

### 8. 代码风格检查

**使用工具**: Read、Grep

如果 skip_style=false（默认），执行以下检查：

1. 检查新代码是否符合项目现有的编码风格：
   - 命名规范（变量、函数、类名）
   - 缩进和格式化（空格 vs Tab，行长度）
   - 注释风格
   - 导入语句顺序
   - 代码组织结构

2. **对比项目中其他文件的代码风格**:
```
# 读取同目录下其他文件作为风格参考
Read: <similar_file_in_same_directory>
```

3. 识别风格不一致的地方

---

### 9. 生成审查报告

**报告模板位置**: `assets/report-template.md`

**报告内容**:
- 按照模板内容章节进行输出。
- 注意要先生成**审查结论**，方便用户快速进行问题审查。
- 如果章节没有问题，就整个章节不要，节省算力。

**报告保存**:
- 如果 save_report=true：
  - 有 commit_id 时：保存到 `review/YYYYMMDD-<commit_id>.md`
  - 无 commit_id 时：保存到 `review/YYYYMMDD-uncommitted.md`
  - 其中 YYYYMMDD 是当前日期，格式：20260126
- 否则：直接输出给用户
- 注意：需要确保 `review` 目录存在，如果不存在则创建

---

## Output

审查结果以 Markdown 报告形式输出，结构遵循 `assets/report-template.md`，包含：基本信息、审查结论（结果/评分/意见）、问题汇总（按严重程度）、详细分析（调用链、接口变更、问题验证、潜在问题、安全、风格等）。

## Present Results to User

1. **先给结论**：审查结果（通过/有条件通过/不通过）、综合评分与各维度评分表。
2. **问题汇总**：按严重/中等/轻微列出问题，含文件与行号。
3. **详细分析**：按模板章节展开，无问题的章节可省略。
4. 若保存报告，告知路径：有 commit_id 时为 `review/YYYYMMDD-<commit_id>.md`，无 commit_id 时为 `review/YYYYMMDD-uncommitted.md`。

## Troubleshooting

| 情况 | 处理方式 |
|------|----------|
| 未提供 commit_id | 审查当前工作区未提交的变更：使用 `git diff`（未暂存）与 `git diff --cached`（已暂存），报告保存为 `review/YYYYMMDD-uncommitted.md` |
| 工作区无未提交变更 | 若 `git diff` 与 `git diff --cached` 均无输出，提示「当前没有未提交的变更，请先修改代码或提供要审查的 commit_id」 |
| commit_id 无效或不存在 | 提示「commit ID 格式无效」或「无法找到 commit」，并说明如何获取正确 ID |
| 当前目录不是 Git 仓库 | 提示「当前目录不是 Git 仓库」，请用户在仓库根目录执行 |
| Merge commit | 检测到多个 parent 时询问用户：仅审查合并结果，或分别审查各父 commit |
| 变更量过大（>1000 行） | 先询问是否继续完整分析；若跳过，仅分析文件列表与函数签名 |
| 报告目录不存在 | 保存前创建 `review` 目录 |
| 跨平台（Windows/Linux/Mac） | 使用 PowerShell 或 bash 对应命令获取 parent 数量与 diff 统计 |

---

## 执行要求

1. **必须使用 git 命令获取变更信息**：无 commit_id 时用 `git diff` 与 `git diff --cached` 获取未提交变更；有 commit_id 时用 git 获取该提交信息与 diff。不要依赖假设。
2. **有 commit_id 时必须先验证**，确保有效后再继续；无 commit_id 时确认在 Git 仓库内且存在未提交变更。
3. **必须分析完整的 diff**，包括所有修改的文件
4. **调用链分析遵循深度限制**，默认 2 层
5. **接口变更检查必须全面**，找出所有使用点
6. **问题检测必须深入**，不能只看表面
7. **代码风格检查必须对比项目现有代码**
8. **如果未发现问题，也要明确说明"未发现问题"**
9. **所有反馈必须具体**，指出具体的文件、行号和问题
10. **报告保存路径**：有 commit_id 时为 `review/YYYYMMDD-<commit_id>.md`，无 commit_id 时为 `review/YYYYMMDD-uncommitted.md`；YYYYMMDD 为当前日期（如 20260126）；若 `review` 目录不存在则创建

## 安装 (End-User Installation)

**Claude Code**:
```bash
cp -r skills/review ~/.claude/skills/
```

**claude.ai**: 将本 skill 加入项目知识库或把 SKILL.md 内容粘贴到对话中。审查需执行 git 命令，请确保环境具备 git 与仓库访问权限。
