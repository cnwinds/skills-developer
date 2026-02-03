#!/bin/bash
# refactor 为指令型 skill，由 Agent 阅读 SKILL.md 后执行，无需命令行参数。
# 此脚本仅满足目录规范，输出 skill 元信息。
set -e
echo '{"skill":"refactor","type":"instruction","usage":"@refactor 或 @refactor <目录名>"}' 
