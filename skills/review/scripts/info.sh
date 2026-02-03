#!/bin/bash
# review 为指令型 skill，由 Agent 阅读 SKILL.md 后执行。
# 此脚本仅满足目录规范，输出 skill 元信息。
set -e
echo '{"skill":"review","type":"instruction","usage":"请审查 commit <commit_id>"}'
