# -*- coding: utf-8 -*-
"""
验证和测试脚本
用于测试优化版的性能和准确性
"""

import json
import time
from datetime import datetime
from neo4j import GraphDatabase
from config import NEO4J_CONFIG
import os


class Validator:
    """验证器"""

    def __init__(self):
        self.driver = GraphDatabase.driver(
            NEO4J_CONFIG['uri'],
            auth=(NEO4J_CONFIG['user'], NEO4J_CONFIG['password'])
        )

    def close(self):
        self.driver.close()

    def test_connection(self):
        """测试数据库连接"""
        print("\n=== 测试数据库连接 ===")
        try:
            with self.driver.session() as session:
                result = session.run("RETURN 1 AS num")
                record = result.single()
                if record['num'] == 1:
                    print("✓ 数据库连接成功")
                    return True
        except Exception as e:
            print(f"✗ 数据库连接失败: {e}")
            return False

    def get_patent_stats(self):
        """获取专利统计"""
        print("\n=== 专利数据统计 ===")
        with self.driver.session() as session:
            # 总数
            result = session.run("""
                MATCH (p:Patent)
                WHERE p.titleZh IS NOT NULL
                RETURN count(p) AS total
            """)
            total = result.single()['total']
            print(f"专利总数: {total:,}")

            # 已分类的绿色技术
            result = session.run("""
                MATCH (p:Patent)-[r:CLASSIFIED_AS]->(g:GreenCategory)
                RETURN count(DISTINCT p) AS classified
            """)
            classified = result.single()['classified']
            print(f"已分类（绿色技术）: {classified:,} ({classified / total * 100:.1f}%)")

            # 已提取地点
            result = session.run("""
                MATCH (p:Patent)-[r:LOCATED_IN]->(l:Location)
                RETURN count(DISTINCT p) AS with_location
            """)
            with_location = result.single()['with_location']
            print(f"已提取地点: {with_location:,} ({with_location / total * 100:.1f}%)")

            # 相似关系
            result = session.run("""
                MATCH (p1:Patent)-[r:SIMILAR_TO]->(p2:Patent)
                RETURN count(r) AS similarities
            """)
            similarities = result.single()['similarities']
            print(f"相似关系: {similarities:,}")

            # 技术领域
            result = session.run("""
                MATCH (p:Patent)-[r:ALSO_BELONGS_TO]->(t:TechDomain)
                RETURN count(DISTINCT p) AS with_tech
            """)
            with_tech = result.single()['with_tech']
            print(f"已分技术领域: {with_tech:,} ({with_tech / total * 100:.1f}%)")

            return {
                'total': total,
                'classified': classified,
                'with_location': with_location,
                'similarities': similarities,
                'with_tech': with_tech
            }

    def check_progress_files(self):
        """检查进度文件"""
        print("\n=== 进度文件检查 ===")
        output_dir = 'llm_output_previous'

        if not os.path.exists(output_dir):
            print("✗ 输出目录不存在")
            return

        # 查找进度文件
        progress_files = [f for f in os.listdir(output_dir) if f.endswith('_progress.json')]
        print(f"发现 {len(progress_files)} 个进度文件:")

        for pf in progress_files:
            filepath = os.path.join(output_dir, pf)
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
                session_id = data['session_id']
                processed = len(data.get('processed_patents', []))
                last_update = data.get('last_update', 'unknown')
                print(f"  - {session_id}: {processed:,} 个专利已处理")
                print(f"    最后更新: {last_update}")

    def sample_results(self, n=5):
        """抽样检查结果质量"""
        print(f"\n=== 抽样检查（{n}个样本）===")

        with self.driver.session() as session:
            # 抽取已分类的专利
            result = session.run("""
                MATCH (p:Patent)-[r:CLASSIFIED_AS]->(g:GreenCategory)
                RETURN p.patentId AS id,
                       p.titleZh AS title,
                       g.code AS category_code,
                       g.name AS category_name,
                       r.confidence AS confidence,
                       r.reasoning AS reasoning
                ORDER BY rand()
                LIMIT $n
            """, n=n)

            print("\n绿色技术分类样本:")
            for i, record in enumerate(result, 1):
                print(f"\n样本 {i}:")
                print(f"  专利ID: {record['id']}")
                print(f"  标题: {record['title'][:50]}...")
                print(f"  分类: {record['category_code']} - {record['category_name']}")
                print(f"  置信度: {record['confidence']:.2f}")
                print(f"  理由: {record['reasoning'][:100]}...")

    def check_cache_size(self):
        """检查缓存大小"""
        print("\n=== 缓存统计 ===")
        cache_dir = 'llm_output_previous/cache'

        if not os.path.exists(cache_dir):
            print("缓存目录不存在")
            return

        cache_files = [f for f in os.listdir(cache_dir) if f.endswith('.json')]
        total_size = sum(os.path.getsize(os.path.join(cache_dir, f)) for f in cache_files)

        print(f"缓存文件数: {len(cache_files)}")
        print(f"缓存大小: {total_size / 1024 / 1024:.2f} MB")
        print(f"预估节省API调用: {len(cache_files):,} 次")

    def performance_test(self, sample_size=10):
        """性能测试"""
        print(f"\n=== 性能测试（{sample_size}个专利）===")

        # 这里可以调用优化版的代码进行小规模测试
        print("注意：完整性能测试需要运行 llm_enhancement_optimized.py")
        print(f"建议：先用 {sample_size} 个专利测试，再扩大规模")

    def compare_with_original(self, session_id):
        """对比原版和优化版的结果"""
        print(f"\n=== 结果对比 ===")
        # 检查指定会话的结果文件
        output_dir = 'llm_output_previous'

        # 查找该会话的所有结果文件
        files = [f for f in os.listdir(output_dir) if f.startswith(session_id)]

        print(f"会话 {session_id} 的结果文件:")
        for f in files:
            filepath = os.path.join(output_dir, f)
            if f.endswith('.json') and 'progress' not in f:
                with open(filepath, 'r', encoding='utf-8') as file:
                    data = json.load(file)
                    print(f"  - {f}: {len(data)} 条记录")


def run_quick_test():
    """快速测试"""
    print("=" * 60)
    print("LLM增强模块 - 快速测试")
    print("=" * 60)

    validator = Validator()

    try:
        # 1. 测试连接
        if not validator.test_connection():
            print("\n⚠ 数据库连接失败，请检查配置")
            return

        # 2. 获取统计
        stats = validator.get_patent_stats()

        # 3. 检查进度
        validator.check_progress_files()

        # 4. 检查缓存
        validator.check_cache_size()

        # 5. 抽样检查
        if stats['classified'] > 0:
            validator.sample_results(n=3)

        print("\n" + "=" * 60)
        print("✓ 测试完成")
        print("=" * 60)

        # 给出建议
        print("\n建议：")
        if stats['classified'] < stats['total'] * 0.1:
            print("1. 运行 llm_enhancement_optimized.py 开始处理专利")
        else:
            print("1. 继续运行 llm_enhancement_optimized.py 处理更多专利")

        print("2. 定期运行此脚本检查进度")
        print("3. 查看日志文件 llm_enhancement_log.txt 了解详情")

    finally:
        validator.close()


def estimate_completion_time(stats):
    """预估完成时间"""
    if stats['classified'] == 0:
        return

    print("\n=== 完成时间预估 ===")
    total = stats['total']
    processed = stats['classified']

    if processed < 10:
        print("样本太小，无法准确预估")
        return

    # 假设处理速度：每批10个专利，30-40秒
    remaining = total - processed
    batches = remaining / 10
    estimated_seconds = batches * 35  # 平均35秒/批

    hours = estimated_seconds / 3600
    days = hours / 24

    print(f"已处理: {processed:,} / {total:,} ({processed / total * 100:.1f}%)")
    print(f"剩余: {remaining:,} 个专利")
    print(f"预计还需: {hours:.1f} 小时 ({days:.1f} 天)")

    if hours < 1:
        print("✓ 很快就能完成！")
    elif hours < 24:
        print("✓ 今天或明天可以完成")
    else:
        print(f"💡 建议: 分 {int(days) + 1} 天完成，每天运行几小时")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == '--full':
        # 完整测试
        validator = Validator()
        try:
            validator.test_connection()
            stats = validator.get_patent_stats()
            validator.check_progress_files()
            validator.check_cache_size()
            validator.sample_results(n=10)
            estimate_completion_time(stats)
        finally:
            validator.close()
    else:
        # 快速测试
        run_quick_test()