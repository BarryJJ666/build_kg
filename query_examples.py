# -*- coding: utf-8 -*-
"""
氢能专利知识图谱 - 查询示例 (增强版)
新增查询功能：
1. 绿色技术分类查询
2. 地点分布查询
3. LLM发现的语义关系查询
"""

from neo4j import GraphDatabase
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PatentGraphQuery:
    """专利图谱查询类"""

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def run_query(self, query, params=None):
        """执行查询并返回结果"""
        with self.driver.session() as session:
            result = session.run(query, params or {})
            return [record.data() for record in result]

    def get_database_stats(self):
        """获取数据库统计信息（包含新增节点类型）"""
        logger.info("=== 数据库统计信息 ===")

        queries = {
            "专利总数": "MATCH (p:Patent) RETURN count(p) as count",
            "实体总数": "MATCH (e:Entity) RETURN count(e) as count",
            "技术领域总数": "MATCH (t:TechDomain) RETURN count(t) as count",
            "绿色分类总数": "MATCH (g:GreenCategory) RETURN count(g) as count",
            "地点节点总数": "MATCH (l:Location) RETURN count(l) as count",
            "日期节点总数": "MATCH (d:Date) RETURN count(d) as count",
            "关系总数": "MATCH ()-[r]->() RETURN count(r) as count",
            "绿色分类关系数": "MATCH ()-[r:CLASSIFIED_AS]->() RETURN count(r) as count",
            "地点关系数": "MATCH ()-[r:LOCATED_IN]->() RETURN count(r) as count",
            "相似关系数": "MATCH ()-[r:SIMILAR_TO]->() RETURN count(r) as count"
        }

        for name, query in queries.items():
            result = self.run_query(query)
            if result:
                logger.info(f"{name}: {result[0]['count']}")

    def query_green_category_distribution(self):
        """查询绿色技术分类分布（新增）"""
        query = """
        MATCH (p:Patent)-[r:CLASSIFIED_AS]->(g:GreenCategory)
        RETURN g.name, g.code, count(p) as patent_count,
               avg(r.confidence) as avg_confidence
        ORDER BY patent_count DESC
        """
        logger.info("\n=== 绿色技术分类分布 ===")
        results = self.run_query(query)
        if not results:
            logger.info("  暂无绿色技术分类数据")
        else:
            for r in results:
                logger.info(f"  {r['g.name']} ({r['g.code']}): {r['patent_count']}件专利, "
                          f"平均置信度: {r['avg_confidence']:.2f}")
        return results

    def query_patents_by_green_category(self, category_code):
        """查询特定绿色技术分类的专利（新增）"""
        query = """
        MATCH (p:Patent)-[r:CLASSIFIED_AS]->(g:GreenCategory {code: $category_code})
        RETURN p.patentId, p.titleZh, r.confidence, r.reasoning
        ORDER BY r.confidence DESC
        LIMIT 10
        """
        logger.info(f"\n=== 绿色技术分类 {category_code} 的专利（前10条）===")
        results = self.run_query(query, {"category_code": category_code})
        if not results:
            logger.info("  未找到该分类的专利")
        else:
            for r in results:
                logger.info(f"  {r['p.titleZh'][:50]}... (置信度: {r['r.confidence']:.2f})")
                if r['r.reasoning']:
                    logger.info(f"    理由: {r['r.reasoning'][:80]}...")
        return results

    def query_location_distribution(self):
        """查询地理位置分布（新增）"""
        query = """
        MATCH (p:Patent)-[:LOCATED_IN]->(l:Location)
        RETURN l.province, count(DISTINCT p) as patent_count
        ORDER BY patent_count DESC
        LIMIT 15
        """
        logger.info("\n=== 专利地理分布（按省份，前15）===")
        results = self.run_query(query)
        if not results:
            logger.info("  暂无地理位置数据")
        else:
            for r in results:
                logger.info(f"  {r['l.province']}: {r['patent_count']}件专利")
        return results

    def query_city_distribution(self, province):
        """查询特定省份的城市分布（新增）"""
        query = """
        MATCH (p:Patent)-[:LOCATED_IN]->(l:Location {province: $province})
        WHERE l.city IS NOT NULL
        RETURN l.city, count(DISTINCT p) as patent_count
        ORDER BY patent_count DESC
        LIMIT 10
        """
        logger.info(f"\n=== {province}的专利城市分布（前10）===")
        results = self.run_query(query, {"province": province})
        if not results:
            logger.info("  未找到该省份的城市数据")
        else:
            for r in results:
                logger.info(f"  {r['l.city']}: {r['patent_count']}件专利")
        return results

    def query_similar_patents(self, patent_id):
        """查询相似专利（LLM发现的关系）"""
        query = """
        MATCH (p1:Patent {patentId: $patent_id})-[r:SIMILAR_TO]->(p2:Patent)
        RETURN p2.patentId, p2.titleZh, 
               r.similarity_score, r.relationship_type, r.dimension
        ORDER BY r.similarity_score DESC
        LIMIT 10
        """
        logger.info(f"\n=== {patent_id} 的相似专利 ===")
        results = self.run_query(query, {"patent_id": patent_id})
        if not results:
            logger.info("  未找到相似专利")
        else:
            for r in results:
                logger.info(f"  {r['p2.titleZh'][:50]}... "
                          f"(相似度: {r['r.similarity_score']:.2f}, "
                          f"类型: {r['r.relationship_type']})")
        return results

    def query_entity_relationships_llm(self):
        """查询LLM发现的实体关系（新增）"""
        query = """
        MATCH (e1:Entity)-[r:RELATED_TO]->(e2:Entity)
        WHERE r.discovered_by = 'LLM'
        RETURN e1.name, e2.name, r.relationship_type, r.confidence
        ORDER BY r.confidence DESC
        LIMIT 15
        """
        logger.info("\n=== LLM发现的实体关系（前15）===")
        results = self.run_query(query)
        if not results:
            logger.info("  暂无LLM发现的实体关系")
        else:
            for r in results:
                logger.info(f"  {r['e1.name']} ←→ {r['e2.name']}: "
                          f"{r['r.relationship_type']} (置信度: {r['r.confidence']:.2f})")
        return results

    def query_patents_by_tech_domain(self, tech_code):
        """查询特定技术领域的专利"""
        query = """
        MATCH (p:Patent)-[:BELONGS_TO_TECH]->(t:TechDomain {code: $tech_code})
        OPTIONAL MATCH (p)-[:OWNED_BY]->(e:Entity)
        RETURN p.pubNumber, p.titleZh, p.patentType, e.name as owner
        LIMIT 10
        """
        logger.info(f"\n=== 技术领域 {tech_code} 的专利（前10条）===")
        results = self.run_query(query, {"tech_code": tech_code})
        for r in results:
            owner = r.get('owner', 'Unknown')
            logger.info(f"  {owner}: {r['p.titleZh']}")
        return results

    def query_entity_patents(self, entity_name):
        """查询某实体的专利技术分布"""
        query = """
        MATCH (e:Entity)<-[:OWNED_BY]-(p:Patent)-[:BELONGS_TO_TECH]->(t:TechDomain)
        WHERE e.name = $entity_name OR e.normalizedName = $entity_name
        RETURN t.nameZh, t.code, count(p) as patent_count
        ORDER BY patent_count DESC
        """
        logger.info(f"\n=== {entity_name} 的专利技术分布 ===")
        results = self.run_query(query, {"entity_name": entity_name})
        if not results:
            logger.info("  未找到该实体的专利")
        else:
            for r in results:
                logger.info(f"  {r['t.nameZh']} ({r['t.code']}): {r['patent_count']}件")
        return results

    def query_entity_green_profile(self, entity_name):
        """查询实体的绿色技术特征（新增）"""
        query = """
        MATCH (e:Entity)<-[:OWNED_BY]-(p:Patent)-[r:CLASSIFIED_AS]->(g:GreenCategory)
        WHERE e.name = $entity_name OR e.normalizedName = $entity_name
        RETURN g.name, g.code, count(p) as patent_count,
               avg(r.confidence) as avg_confidence
        ORDER BY patent_count DESC
        """
        logger.info(f"\n=== {entity_name} 的绿色技术特征 ===")
        results = self.run_query(query, {"entity_name": entity_name})
        if not results:
            logger.info("  未找到该实体的绿色技术分类数据")
        else:
            for r in results:
                logger.info(f"  {r['g.name']}: {r['patent_count']}件专利, "
                          f"平均置信度: {r['avg_confidence']:.2f}")
        return results

    def query_tech_domain_distribution(self):
        """查询技术领域专利分布"""
        query = """
        MATCH (p:Patent)-[:BELONGS_TO_TECH]->(t:TechDomain)
        WHERE t.level = 2
        RETURN t.nameZh, t.code, count(p) as patent_count
        ORDER BY patent_count DESC
        LIMIT 10
        """
        logger.info("\n=== L2级技术领域专利分布（前10）===")
        results = self.run_query(query)
        for r in results:
            logger.info(f"  {r['t.nameZh']} ({r['t.code']}): {r['patent_count']}件")
        return results

    def query_top_entities(self, limit=10):
        """查询专利数量最多的实体"""
        query = """
        MATCH (e:Entity)<-[:OWNED_BY]-(p:Patent)
        WHERE e.name IS NOT NULL
        RETURN e.name, e.type, count(p) as patent_count
        ORDER BY patent_count DESC
        LIMIT $limit
        """
        logger.info(f"\n=== 专利数量前{limit}的实体 ===")
        results = self.run_query(query, {"limit": limit})
        for i, r in enumerate(results, 1):
            logger.info(f"  {i}. {r['e.name']} ({r['e.type']}): {r['patent_count']}件")
        return results

    def query_patent_family(self, pub_number):
        """查询专利同族"""
        query = """
        MATCH (p:Patent {pubNumber: $pub_number})-[:HAS_FAMILY]-(f:Patent)
        RETURN f.pubNumber, f.titleZh, f.publicCountry
        """
        logger.info(f"\n=== {pub_number} 的同族专利 ===")
        results = self.run_query(query, {"pub_number": pub_number})
        for r in results:
            logger.info(f"  {r['f.pubNumber']} ({r['f.publicCountry']}): {r['f.titleZh']}")
        return results

    def query_tech_domain_tree(self, root_code="H1"):
        """查询技术领域树"""
        query = """
        MATCH path = (child:TechDomain)-[:SUBDOMAIN_OF*]->(parent:TechDomain {code: $root_code})
        RETURN child.code, child.nameZh, child.level
        ORDER BY child.code
        """
        logger.info(f"\n=== 技术领域树 {root_code} ===")
        results = self.run_query(query, {"root_code": root_code})
        for r in results:
            indent = "  " * (r['child.level'] - 1)
            logger.info(f"{indent}{r['child.code']}: {r['child.nameZh']}")
        return results

    def query_patents_by_date_range(self, start_date, end_date):
        """查询日期范围内的专利申请趋势"""
        query = """
        MATCH (p:Patent)-[:FILED_ON]->(d:Date)
        WHERE d.dateValue >= $start_date AND d.dateValue <= $end_date
        RETURN substring(d.dateValue, 0, 7) as month, count(p) as patent_count
        ORDER BY month
        """
        logger.info(f"\n=== {start_date} 至 {end_date} 专利申请趋势 ===")
        results = self.run_query(query, {"start_date": start_date, "end_date": end_date})
        for r in results:
            logger.info(f"  {r['month']}: {r['patent_count']}件")
        return results

    def query_collaboration_network(self, entity_name):
        """查询实体的合作网络"""
        query = """
        MATCH (e1:Entity)<-[:OWNED_BY]-(p:Patent)-[:OWNED_BY]->(e2:Entity)
        WHERE (e1.name = $entity_name OR e1.normalizedName = $entity_name)
          AND e1 <> e2
          AND e2.name IS NOT NULL
        RETURN e2.name, count(p) as collaboration_count
        ORDER BY collaboration_count DESC
        LIMIT 10
        """
        logger.info(f"\n=== {entity_name} 的合作伙伴（共同持有专利）===")
        results = self.run_query(query, {"entity_name": entity_name})
        if not results:
            logger.info("  未找到合作伙伴")
        else:
            for r in results:
                logger.info(f"  {r['e2.name']}: {r['collaboration_count']}件共同专利")
        return results

    def query_patent_transfers(self):
        """查询专利转让活跃度"""
        query = """
        MATCH (p:Patent)-[:ASSIGNED_FROM]->(from:Entity)
        MATCH (p)-[:ASSIGNED_TO]->(to:Entity)
        WHERE from.name IS NOT NULL AND to.name IS NOT NULL
        RETURN from.name, to.name, count(p) as transfer_count
        ORDER BY transfer_count DESC
        LIMIT 10
        """
        logger.info("\n=== 专利转让活跃度（前10）===")
        results = self.run_query(query)
        if not results:
            logger.info("  未找到转让记录")
        else:
            for r in results:
                logger.info(f"  {r['from.name']} → {r['to.name']}: {r['transfer_count']}件")
        return results

    def query_cross_analysis(self, province, green_category_code):
        """交叉分析：特定省份的绿色技术分类（新增）"""
        query = """
        MATCH (p:Patent)-[:LOCATED_IN]->(l:Location {province: $province})
        MATCH (p)-[r:CLASSIFIED_AS]->(g:GreenCategory {code: $green_category_code})
        OPTIONAL MATCH (p)-[:OWNED_BY]->(e:Entity)
        RETURN p.titleZh, e.name, r.confidence
        ORDER BY r.confidence DESC
        LIMIT 10
        """
        logger.info(f"\n=== {province}的{green_category_code}类专利（前10）===")
        results = self.run_query(query, {
            "province": province,
            "green_category_code": green_category_code
        })
        if not results:
            logger.info("  未找到匹配的专利")
        else:
            for r in results:
                logger.info(f"  {r['p.titleZh'][:50]}... - {r['e.name']} "
                          f"(置信度: {r['r.confidence']:.2f})")
        return results

    def check_data_quality(self):
        """检查数据质量"""
        logger.info("\n=== 数据质量检查 ===")

        # 检查空值Entity
        query_null_entities = """
        MATCH (e:Entity)
        WHERE e.name IS NULL OR e.name = ''
        RETURN count(e) as null_count
        """
        result = self.run_query(query_null_entities)
        null_count = result[0]['null_count'] if result else 0
        logger.info(f"空name的Entity节点数: {null_count}")

        # 检查有效Entity样例
        query_valid_entities = """
        MATCH (e:Entity)<-[:OWNED_BY]-(p:Patent)
        WHERE e.name IS NOT NULL
        RETURN e.name, e.normalizedName, count(p) as patent_count
        ORDER BY patent_count DESC
        LIMIT 5
        """
        results = self.run_query(query_valid_entities)
        logger.info("有效Entity样例:")
        for r in results:
            logger.info(f"  {r['e.name']} (规范名: {r['e.normalizedName']}) - {r['patent_count']}件专利")

        return null_count


def main():
    """运行示例查询"""
    # Neo4j连接配置
    NEO4J_URI = "bolt://8.130.70.72:21099"
    NEO4J_USER = "neo4j"
    NEO4J_PASSWORD = "zyz_password"

    # 创建查询对象
    querier = PatentGraphQuery(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)

    try:
        # 1. 数据库统计（包含新增节点）
        querier.get_database_stats()

        # 2. 数据质量检查
        querier.check_data_quality()

        # 3. 绿色技术分类分布（新增）
        querier.query_green_category_distribution()

        # 4. 地理位置分布（新增）
        querier.query_location_distribution()

        # 5. LLM发现的实体关系（新增）
        querier.query_entity_relationships_llm()

        # 6. 技术领域分布
        querier.query_tech_domain_distribution()

        # 7. 头部实体
        querier.query_top_entities(10)

        # 8. 查询特定技术领域
        querier.query_patents_by_tech_domain("H1.1")

        # 9. 技术领域树
        querier.query_tech_domain_tree("H1")

        # 10. 专利申请趋势（最近3年）
        querier.query_patents_by_date_range("2022-01-01", "2025-12-31")

        # 11. 专利转让活跃度
        querier.query_patent_transfers()

        # 如果有具体数据，可以查询：
        # querier.query_patents_by_green_category("GT1")
        # querier.query_city_distribution("江苏")
        # querier.query_entity_green_profile("具体公司名称")
        # querier.query_cross_analysis("江苏", "GT1")

    except Exception as e:
        logger.error(f"查询错误: {e}")
    finally:
        querier.close()


if __name__ == "__main__":
    main()