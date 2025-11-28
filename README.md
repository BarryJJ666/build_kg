
# 创建那些不用LLM的节点（已创建好）
python import_patents.py --clear

python import_patents.py


# 1. 默认行为（自动恢复最新会话）
python llm_generate_json.py

# 2. 查看所有会话
python llm_generate_json.py --list

# 3. 恢复指定会话
python llm_generate_json.py --session 20251127_172737

# 4. 强制创建新会话
python llm_generate_json.py --new

# 5. 查看帮助
python llm_generate_json.py --help

# 将json内容全部导入neo4j
python llm_import_to_neo4j.py