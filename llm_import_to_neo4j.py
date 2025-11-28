# -*- coding: utf-8 -*-
"""
LLM数据导入Neo4j模块（修改版）
功能:
1. 从JSON文件读取LLM生成的数据
2. 批量导入到Neo4j数据库
3. 创建相应的关系
"""

import os
import json
import logging
from datetime import datetime
from neo4j import GraphDatabase
from config import NEO4J_CONFIG, LLM_ENHANCE_CONFIG

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('llm_import_to_neo4j.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class Neo4jImporter:
    """Neo4j导入器 - 从JSON导入到数据库"""

    def __init__(self, session_id: str):
        """初始化"""
        self.session_id = session_id
        self.output_dir = LLM_ENHANCE_CONFIG['output_dir']

        # Neo4j连接
        self.driver = GraphDatabase.driver(
            NEO4J_CONFIG['uri'],
            auth=(NEO4J_CONFIG['user'], NEO4J_CONFIG['password']),
            max_connection_pool_size=100,
            connection_timeout=60
        )

        # 导入统计
        self.import_stats = {
            'green_classifications': 0,
            'tech_classifications': 0,
            'entity_locations': 0,
            'errors': []
        }

        logger.info(f"初始化导入器 - 会话ID: {session_id}")

    def close(self):
        """关闭连接"""
        self.driver.close()

    def load_json_data(self, data_type: str):
        """加载JSON数据"""
        filename = f"{self.session_id}_{data_type}.json"
        filepath = os.path.join(self.output_dir, filename)

        if not os.path.exists(filepath):
            logger.warning(f"文件不存在: {filepath}")
            return []

        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
                logger.info(f"✓ 加载 {data_type}: {len(data)} 条记录")
                return data
        except Exception as e:
            logger.error(f"加载 {data_type} 失败: {e}")
            return []

    def import_green_classifications(self):
        """导入绿色技术分类"""
        logger.info("\n" + "=" * 60)
        logger.info("导入绿色技术分类")
        logger.info("=" * 60)

        data = self.load_json_data('green_classifications')
        if not data:
            logger.info("没有绿色技术分类数据需要导入")
            return

        with self.driver.session() as session:
            batch_size = 1000
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]

                try:
                    query = """
                    UNWIND $batch AS item
                    MATCH (p:Patent {patentId: item.patent_id})
                    MATCH (g:GreenTechCategory {code: item.category_code})
                    MERGE (p)-[r:CATEGORIZED_AS_GREEN]->(g)
                    SET r.confidence = item.confidence,
                        r.reasoning = item.reasoning,
                        r.discovered_by = 'LLM',
                        r.discovered_at = datetime()
                    """
                    result = session.run(query, batch=batch)
                    result.consume()

                    self.import_stats['green_classifications'] += len(batch)
                    logger.info(f"已导入: {self.import_stats['green_classifications']}/{len(data)}")

                except Exception as e:
                    logger.error(f"导入绿色分类批次失败: {e}")
                    self.import_stats['errors'].append(f"绿色分类批次{i}-{i + batch_size}失败: {e}")

        logger.info(f"✓ 绿色技术分类导入完成: {self.import_stats['green_classifications']} 条")

    def import_tech_classifications(self):
        """导入技术领域分类"""
        logger.info("\n" + "=" * 60)
        logger.info("导入技术领域分类")
        logger.info("=" * 60)

        data = self.load_json_data('tech_classifications')
        if not data:
            logger.info("没有技术领域分类数据需要导入")
            return

        # 展开数据：每个patent可能有多个tech_domains
        expanded = []
        for item in data:
            patent_id = item.get('patent_id')
            for domain in item.get('tech_domains', []):
                expanded.append({
                    'patent_id': patent_id,
                    'tech_code': domain.get('code'),
                    'level': domain.get('level'),
                    'confidence': domain.get('confidence', 0.8),
                    'reasoning': domain.get('reasoning', '')
                })

        if not expanded:
            logger.info("没有技术领域分类数据需要导入")
            return

        logger.info(f"展开后共 {len(expanded)} 条技术领域关系")

        with self.driver.session() as session:
            batch_size = 1000
            for i in range(0, len(expanded), batch_size):
                batch = expanded[i:i + batch_size]

                try:
                    query = """
                    UNWIND $batch AS item
                    MATCH (p:Patent {patentId: item.patent_id})
                    MATCH (t:TechDomain {code: item.tech_code})
                    MERGE (p)-[r:BELONGS_TO_TECH]->(t)
                    SET r.level = item.level,
                        r.confidence = item.confidence,
                        r.reasoning = item.reasoning,
                        r.discovered_by = 'LLM',
                        r.discovered_at = datetime(),
                        r.source = 'LLM-refined'
                    """
                    result = session.run(query, batch=batch)
                    result.consume()

                    self.import_stats['tech_classifications'] += len(batch)
                    logger.info(f"已导入: {self.import_stats['tech_classifications']}/{len(expanded)}")

                except Exception as e:
                    logger.error(f"导入技术领域批次失败: {e}")
                    self.import_stats['errors'].append(f"技术领域批次{i}-{i + batch_size}失败: {e}")

        logger.info(f"✓ 技术领域分类导入完成: {self.import_stats['tech_classifications']} 条")

    def import_entity_locations(self):
        """导入实体地理位置数据"""
        logger.info("\n" + "=" * 60)
        logger.info("导入实体地理位置数据")
        logger.info("=" * 60)

        data = self.load_json_data('entity_locations')
        if not data:
            logger.info("没有实体地理位置数据需要导入")
            return

        # 过滤掉confidence=0的数据
        valid_data = [item for item in data if item.get('confidence', 0) > 0 and item.get('province')]

        if not valid_data:
            logger.info("没有有效的地理位置数据需要导入")
            return

        logger.info(f"有效地理位置数据: {len(valid_data)} 条")

        with self.driver.session() as session:
            # 创建省份节点和关系
            logger.info("创建省份节点和关系...")
            batch_size = 1000
            for i in range(0, len(valid_data), batch_size):
                batch = valid_data[i:i + batch_size]

                try:
                    query = """
                    UNWIND $batch AS item
                    MATCH (e:Entity {normalizedName: item.normalized_name})
                    MERGE (p:Province {name: item.province})
                    MERGE (e)-[r:LOCATED_IN_PROVINCE]->(p)
                    SET r.confidence = item.confidence,
                        r.source = item.source,
                        r.discovered_by = 'LLM',
                        r.discovered_at = datetime()
                    """
                    result = session.run(query, batch=batch)
                    result.consume()

                    logger.info(f"省份关系: {i + len(batch)}/{len(valid_data)}")

                except Exception as e:
                    logger.error(f"导入省份批次失败: {e}")

            # 创建城市节点和关系
            city_data = [item for item in valid_data if item.get('city')]
            if city_data:
                logger.info("创建城市节点和关系...")
                for i in range(0, len(city_data), batch_size):
                    batch = city_data[i:i + batch_size]

                    try:
                        query = """
                        UNWIND $batch AS item
                        MATCH (e:Entity {normalizedName: item.normalized_name})
                        MERGE (c:City {fullName: item.province + '-' + item.city})
                        ON CREATE SET c.name = item.city, c.province = item.province
                        MERGE (e)-[r:LOCATED_IN_CITY]->(c)
                        SET r.confidence = item.confidence,
                            r.source = item.source,
                            r.discovered_by = 'LLM',
                            r.discovered_at = datetime()
                        """
                        result = session.run(query, batch=batch)
                        result.consume()

                        logger.info(f"城市关系: {i + len(batch)}/{len(city_data)}")

                    except Exception as e:
                        logger.error(f"导入城市批次失败: {e}")

            # 创建区县节点和关系
            district_data = [item for item in valid_data if item.get('district')]
            if district_data:
                logger.info("创建区县节点和关系...")
                for i in range(0, len(district_data), batch_size):
                    batch = district_data[i:i + batch_size]

                    try:
                        query = """
                        UNWIND $batch AS item
                        MATCH (e:Entity {normalizedName: item.normalized_name})
                        MERGE (d:District {fullName: item.province + '-' + item.city + '-' + item.district})
                        ON CREATE SET d.name = item.district,
                                     d.city = item.city,
                                     d.province = item.province
                        MERGE (e)-[r:LOCATED_IN_DISTRICT]->(d)
                        SET r.confidence = item.confidence,
                            r.source = item.source,
                            r.discovered_by = 'LLM',
                            r.discovered_at = datetime()
                        """
                        result = session.run(query, batch=batch)
                        result.consume()

                        logger.info(f"区县关系: {i + len(batch)}/{len(district_data)}")

                    except Exception as e:
                        logger.error(f"导入区县批次失败: {e}")

            self.import_stats['entity_locations'] = len(valid_data)

        logger.info(f"✓ 实体地理位置数据导入完成: {len(valid_data)} 条")

    def verify_import(self):
        """验证导入结果"""
        logger.info("\n" + "=" * 60)
        logger.info("验证导入结果")
        logger.info("=" * 60)

        with self.driver.session() as session:
            queries = {
                "绿色技术分类关系": "MATCH ()-[r:CATEGORIZED_AS_GREEN]->() WHERE r.discovered_by = 'LLM' RETURN count(r) as count",
                "技术领域关系(LLM)": "MATCH ()-[r:BELONGS_TO_TECH]->() WHERE r.discovered_by = 'LLM' RETURN count(r) as count",
                "省份关系(LLM)": "MATCH ()-[r:LOCATED_IN_PROVINCE]->() WHERE r.discovered_by = 'LLM' RETURN count(r) as count",
                "城市关系(LLM)": "MATCH ()-[r:LOCATED_IN_CITY]->() WHERE r.discovered_by = 'LLM' RETURN count(r) as count",
                "区县关系(LLM)": "MATCH ()-[r:LOCATED_IN_DISTRICT]->() WHERE r.discovered_by = 'LLM' RETURN count(r) as count",
                "技术领域节点": "MATCH (t:TechDomain) RETURN count(t) as count",
                "绿色技术分类节点": "MATCH (g:GreenTechCategory) RETURN count(g) as count",
                "省份节点": "MATCH (p:Province) RETURN count(p) as count",
                "城市节点": "MATCH (c:City) RETURN count(c) as count",
                "区县节点": "MATCH (d:District) RETURN count(d) as count"
            }

            for name, query in queries.items():
                try:
                    result = session.run(query)
                    count = result.single()['count']
                    logger.info(f"  {name}: {count} 条/个")
                except Exception as e:
                    logger.error(f"验证 {name} 失败: {e}")

    def import_all(self):
        """导入所有数据"""
        logger.info("=" * 60)
        logger.info("开始从JSON导入到Neo4j")
        logger.info(f"会话ID: {self.session_id}")
        logger.info("=" * 60)

        start_time = datetime.now()

        # 依次导入各类数据
        self.import_green_classifications()
        self.import_tech_classifications()
        self.import_entity_locations()

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        # 验证导入
        self.verify_import()

        # 打印总结
        logger.info("\n" + "=" * 60)
        logger.info("导入完成总结")
        logger.info("=" * 60)
        logger.info(f"会话ID: {self.session_id}")
        logger.info(f"耗时: {duration:.2f} 秒")
        logger.info(f"\n导入统计:")
        logger.info(f"  - 绿色技术分类: {self.import_stats['green_classifications']} 条")
        logger.info(f"  - 技术领域分类: {self.import_stats['tech_classifications']} 条")
        logger.info(f"  - 实体地理位置: {self.import_stats['entity_locations']} 条")

        if self.import_stats['errors']:
            logger.warning(f"\n错误数量: {len(self.import_stats['errors'])}")
            for error in self.import_stats['errors'][:10]:  # 只显示前10个错误
                logger.warning(f"  - {error}")
        else:
            logger.info("\n✓ 没有错误!")

        logger.info("=" * 60)


def list_available_sessions(output_dir: str = None):
    """列出所有可用的会话"""
    output_dir = output_dir or LLM_ENHANCE_CONFIG['output_dir']

    if not os.path.exists(output_dir):
        logger.info(f"输出目录不存在: {output_dir}")
        return []

    # 查找所有progress.json文件
    sessions = []
    for filename in os.listdir(output_dir):
        if filename.endswith('_progress.json'):
            session_id = filename.replace('_progress.json', '')

            # 读取进度信息
            filepath = os.path.join(output_dir, filename)
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    progress = json.load(f)
                    sessions.append({
                        'session_id': session_id,
                        'processed_patents': len(progress.get('processed_patents', [])),
                        'processed_entities': len(progress.get('processed_entities', [])),
                        'last_update': progress.get('last_update', 'Unknown')
                    })
            except:
                pass

    return sorted(sessions, key=lambda x: x['session_id'], reverse=True)


def main():
    """主函数"""
    import sys

    session_id = None
    list_sessions = False

    if len(sys.argv) > 1:
        if sys.argv[1] == '--session':
            session_id = sys.argv[2] if len(sys.argv) > 2 else None
        elif sys.argv[1] == '--list':
            list_sessions = True

    if list_sessions:
        logger.info("可用的会话:")
        sessions = list_available_sessions()
        if not sessions:
            logger.info("  (没有找到会话)")
        else:
            for sess in sessions:
                logger.info(f"  - {sess['session_id']}")
                logger.info(f"    已处理专利: {sess['processed_patents']}")
                logger.info(f"    已处理实体: {sess['processed_entities']}")
                logger.info(f"    最后更新: {sess['last_update']}")
        return

    if not session_id:
        # 尝试使用最新的会话
        sessions = list_available_sessions()
        if sessions:
            session_id = sessions[0]['session_id']
            logger.info(f"使用最新会话: {session_id}")
        else:
            logger.error("错误: 请指定会话ID")
            logger.info("用法: python llm_import_to_neo4j.py --session {session_id}")
            logger.info("或者: python llm_import_to_neo4j.py --list  # 列出所有会话")
            return

    importer = Neo4jImporter(session_id)

    try:
        importer.import_all()
        logger.info("\n✓ 所有数据已成功导入Neo4j!")

    except Exception as e:
        logger.error(f"导入失败: {e}", exc_info=True)
        raise
    finally:
        importer.close()


if __name__ == "__main__":
    main()