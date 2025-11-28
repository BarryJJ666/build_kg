# -*- coding: utf-8 -*-
"""
氢能知识图谱 - 修复版 Cypher查询语句
解决了语法错误，优化了查询结果
"""

# ============================================================================
# 查询1: 识别特定技术领域的非北京领先区域和创新主体
# ============================================================================

QUERY_1_NON_BEIJING_LEADERS = """
// 查询1: 识别特定技术领域的非北京领先区域和创新主体
// 参数: $techCode (例如: 'H1.1' 或 'H3.1')

MATCH (p:Patent)-[:BELONGS_TO_TECH]->(t:TechDomain)
WHERE t.code STARTS WITH $techCode

// 获取专利的申请人
MATCH (p)-[:APPLIED_BY]->(e:Entity)
OPTIONAL MATCH (e)-[:LOCATED_IN_PROVINCE]->(prov:Province)

// 处理未识别位置的情况：从实体地址中提取省份
WITH p, e, 
     COALESCE(prov.name, 
              CASE 
                WHEN e.address CONTAINS '北京' THEN '北京市'
                WHEN e.address CONTAINS '上海' THEN '上海市'
                WHEN e.address CONTAINS '天津' THEN '天津市'
                WHEN e.address CONTAINS '重庆' THEN '重庆市'
                WHEN e.address CONTAINS '四川' THEN '四川省'
                WHEN e.address CONTAINS '江苏' THEN '江苏省'
                WHEN e.address CONTAINS '浙江' THEN '浙江省'
                WHEN e.address CONTAINS '广东' THEN '广东省'
                WHEN e.address CONTAINS '山东' THEN '山东省'
                WHEN e.address CONTAINS '陕西' THEN '陕西省'
                WHEN e.address CONTAINS '湖北' THEN '湖北省'
                WHEN e.address CONTAINS '湖南' THEN '湖南省'
                WHEN e.address CONTAINS '河南' THEN '河南省'
                WHEN e.address CONTAINS '河北' THEN '河北省'
                WHEN e.address CONTAINS '安徽' THEN '安徽省'
                WHEN e.address CONTAINS '福建' THEN '福建省'
                WHEN e.address CONTAINS '辽宁' THEN '辽宁省'
                WHEN e.address CONTAINS '吉林' THEN '吉林省'
                WHEN e.address CONTAINS '黑龙江' THEN '黑龙江省'
                WHEN e.address CONTAINS '山西' THEN '山西省'
                WHEN e.address CONTAINS '内蒙古' THEN '内蒙古自治区'
                WHEN e.address CONTAINS '新疆' THEN '新疆维吾尔自治区'
                ELSE '未知'
              END) as provinceName

// 过滤掉北京和未知位置
WHERE provinceName <> '北京市' AND provinceName <> '未知'

// 统计每个省份和主体的专利数量
WITH provinceName, e, count(DISTINCT p) as patentCount
ORDER BY provinceName, patentCount DESC

// 对每个省份，取前10个创新主体
WITH provinceName, 
     collect({entity: e, count: patentCount})[..10] as topEntities,
     sum(patentCount) as totalPatents

// 只返回专利数量>=5的省份
WHERE totalPatents >= 5

RETURN 
    provinceName as 省份,
    totalPatents as 专利总数,
    [entity IN topEntities | {
        名称: entity.entity.name,
        专利数: entity.count,
        类型: entity.entity.type,
        地址: entity.entity.address
    }] as 领先创新主体
ORDER BY totalPatents DESC
LIMIT 20
"""

QUERY_1_EXAMPLE_PARAMS = {
    'techCode': 'H1.1'  # 电解水制氢
}

# ============================================================================
# 查询2: 查询非北京区域在特定绿色技术的专利分布
# ============================================================================

QUERY_2_GREEN_TECH_DISTRIBUTION = """
// 查询2: 查询非北京区域在特定绿色技术的专利分布
// 参数: $greenCode (例如: 'GT1' - 零碳使能型)

MATCH (p:Patent)-[:CLASSIFIED_AS_GREEN]->(g:GreenTechCategory)
WHERE g.code = $greenCode

// 获取专利的地理位置（通过申请人）
MATCH (p)-[:APPLIED_BY]->(e:Entity)

// 从地址中提取省份（因为地理位置识别率低）
WITH p, e, g,
     CASE 
       WHEN e.address CONTAINS '北京' THEN '北京市'
       WHEN e.address CONTAINS '上海' THEN '上海市'
       WHEN e.address CONTAINS '天津' THEN '天津市'
       WHEN e.address CONTAINS '重庆' THEN '重庆市'
       WHEN e.address CONTAINS '四川' THEN '四川省'
       WHEN e.address CONTAINS '江苏' THEN '江苏省'
       WHEN e.address CONTAINS '浙江' THEN '浙江省'
       WHEN e.address CONTAINS '广东' THEN '广东省'
       WHEN e.address CONTAINS '山东' THEN '山东省'
       WHEN e.address CONTAINS '陕西' THEN '陕西省'
       WHEN e.address CONTAINS '湖北' THEN '湖北省'
       WHEN e.address CONTAINS '湖南' THEN '湖南省'
       WHEN e.address CONTAINS '河南' THEN '河南省'
       WHEN e.address CONTAINS '河北' THEN '河北省'
       WHEN e.address CONTAINS '安徽' THEN '安徽省'
       WHEN e.address CONTAINS '福建' THEN '福建省'
       WHEN e.address CONTAINS '辽宁' THEN '辽宁省'
       WHEN e.address CONTAINS '吉林' THEN '吉林省'
       WHEN e.address CONTAINS '黑龙江' THEN '黑龙江省'
       WHEN e.address CONTAINS '山西' THEN '山西省'
       WHEN e.address CONTAINS '江西' THEN '江西省'
       WHEN e.address CONTAINS '云南' THEN '云南省'
       WHEN e.address CONTAINS '贵州' THEN '贵州省'
       WHEN e.address CONTAINS '广西' THEN '广西壮族自治区'
       WHEN e.address CONTAINS '内蒙古' THEN '内蒙古自治区'
       WHEN e.address CONTAINS '新疆' THEN '新疆维吾尔自治区'
       WHEN e.address CONTAINS '甘肃' THEN '甘肃省'
       WHEN e.address CONTAINS '青海' THEN '青海省'
       ELSE '未知'
     END as provinceName

// 过滤掉北京和未知位置
WHERE provinceName <> '北京市' AND provinceName <> '未知'

// 同时获取技术领域信息
OPTIONAL MATCH (p)-[:BELONGS_TO_TECH]->(t:TechDomain)
WHERE t.level = 2  // 二级技术领域

// 按省份统计
WITH provinceName, 
     count(DISTINCT p) as patentCount,
     collect(DISTINCT t.nameZh)[..5] as techDomains,
     collect(DISTINCT e.name)[..5] as sampleEntities

WHERE patentCount >= 1  // 降低阈值，确保有结果

RETURN 
    provinceName as 省份,
    patentCount as 专利总数,
    techDomains as 主要技术领域,
    sampleEntities as 样本企业
ORDER BY patentCount DESC
LIMIT 20
"""

QUERY_2_EXAMPLE_PARAMS = {
    'greenCode': 'GT1'  # GT1:零碳使能型
}

# ============================================================================
# 查询3: 识别高绿色程度但专利密度低的技术领域（修复版）
# ============================================================================

QUERY_3_HIGH_GREEN_LOW_DENSITY = """
// 查询3: 识别高绿色程度但专利密度低的技术领域
// 识别在GT1(零碳使能型)和GT2(低碳过渡型)中专利较少的技术领域

// 统计每个技术领域的专利数和绿色分类分布
MATCH (t:TechDomain)<-[:BELONGS_TO_TECH]-(p:Patent)
WHERE t.level = 3  // 只看三级技术领域（最具体）

// 获取绿色分类信息
OPTIONAL MATCH (p)-[:CLASSIFIED_AS_GREEN]->(g:GreenTechCategory)

WITH t,
     count(DISTINCT p) as totalPatents,
     sum(CASE WHEN g.code = 'GT1' THEN 1 ELSE 0 END) as gt1Count,
     sum(CASE WHEN g.code = 'GT2' THEN 1 ELSE 0 END) as gt2Count,
     sum(CASE WHEN g.code = 'GT3' THEN 1 ELSE 0 END) as gt3Count,
     sum(CASE WHEN g.code IN ['GT1', 'GT2'] THEN 1 ELSE 0 END) as highGreenCount

// 计算高绿色度比例 - 关键修复：在这里计算比例并保留所有需要的变量
WITH t, totalPatents, gt1Count, gt2Count, gt3Count, highGreenCount,
     CASE WHEN totalPatents > 0 
          THEN toFloat(highGreenCount) / totalPatents 
          ELSE 0 END as highGreenRatio

// 过滤条件：
// 1. 高绿色度比例 >= 30% (GT1+GT2占比高)
// 2. 专利总数 < 100 (密度低，有发展空间)
// 3. 专利总数 >= 5 (不要太冷门)
WHERE highGreenRatio >= 0.3 
  AND totalPatents < 100 
  AND totalPatents >= 5

// 获取父级技术领域信息
OPTIONAL MATCH (parent:TechDomain)-[:HAS_CHILD]->(t)

// 最终返回 - 再次包含所有需要的变量
WITH t, totalPatents, gt1Count, gt2Count, highGreenRatio, 
     collect(DISTINCT parent.nameZh)[0] as parentName

RETURN 
    t.code as 技术领域代码,
    t.nameZh as 技术领域名称,
    t.nameEn as 英文名称,
    totalPatents as 专利总数,
    gt1Count as GT1零碳专利数,
    gt2Count as GT2低碳专利数,
    round(highGreenRatio * 100, 1) as 高绿色度百分比,
    parentName as 所属二级领域
ORDER BY highGreenRatio DESC, totalPatents ASC
LIMIT 20
"""

# ============================================================================
# 查询4: 识别北京专利占比低的高绿色技术领域（修复版）
# ============================================================================

QUERY_4_LOW_BEIJING_HIGH_GREEN = """
// 查询4: 识别北京专利占比低的高绿色技术领域
// 找出非北京区域有优势的绿色技术机会

// 统计每个技术领域的总专利数和北京专利数
MATCH (t:TechDomain)<-[:BELONGS_TO_TECH]-(p:Patent)
WHERE t.level IN [2, 3]  // 二级或三级技术领域

// 获取绿色分类
OPTIONAL MATCH (p)-[:CLASSIFIED_AS_GREEN]->(g:GreenTechCategory)

// 获取地理位置 - 从地址提取，因为关系识别率低
MATCH (p)-[:APPLIED_BY]->(e:Entity)
WITH t, p, g, e,
     CASE 
       WHEN e.address CONTAINS '北京' THEN '北京市'
       ELSE '其他'
     END as region

WITH t,
     count(DISTINCT p) as totalPatents,
     sum(CASE WHEN region = '北京市' THEN 1 ELSE 0 END) as beijingPatents,
     sum(CASE WHEN g.code IN ['GT1', 'GT2'] THEN 1 ELSE 0 END) as highGreenCount

// 计算北京占比和高绿色度比例
WITH t, totalPatents, beijingPatents, highGreenCount,
     CASE WHEN totalPatents > 0 
          THEN toFloat(beijingPatents) / totalPatents 
          ELSE 0 END as beijingRatio,
     CASE WHEN totalPatents > 0 
          THEN toFloat(highGreenCount) / totalPatents 
          ELSE 0 END as highGreenRatio

// 过滤条件：
// 1. 专利总数 >= 10 (有一定规模)
// 2. 高绿色度比例 >= 20% (降低阈值，确保有结果)
// 3. 北京占比 < 50% (降低阈值)
WHERE totalPatents >= 10 
  AND highGreenRatio >= 0.20 
  AND beijingRatio < 0.50

// 获取非北京的主要创新区域
MATCH (t)<-[:BELONGS_TO_TECH]-(p2:Patent)-[:APPLIED_BY]->(e2:Entity)
WHERE NOT e2.address CONTAINS '北京'

WITH t, totalPatents, beijingPatents, highGreenCount, beijingRatio, highGreenRatio,
     CASE 
       WHEN e2.address CONTAINS '上海' THEN '上海市'
       WHEN e2.address CONTAINS '天津' THEN '天津市'
       WHEN e2.address CONTAINS '重庆' THEN '重庆市'
       WHEN e2.address CONTAINS '四川' THEN '四川省'
       WHEN e2.address CONTAINS '江苏' THEN '江苏省'
       WHEN e2.address CONTAINS '浙江' THEN '浙江省'
       WHEN e2.address CONTAINS '广东' THEN '广东省'
       WHEN e2.address CONTAINS '山东' THEN '山东省'
       WHEN e2.address CONTAINS '陕西' THEN '陕西省'
       WHEN e2.address CONTAINS '湖北' THEN '湖北省'
       WHEN e2.address CONTAINS '湖南' THEN '湖南省'
       WHEN e2.address CONTAINS '河南' THEN '河南省'
       ELSE '其他'
     END as nonBeijingProvince,
     count(DISTINCT p2) as provincePatents

WHERE nonBeijingProvince <> '其他'

// 找出每个技术领域的前3个非北京省份
WITH t, totalPatents, beijingPatents, highGreenCount, beijingRatio, highGreenRatio,
     collect({省份: nonBeijingProvince, 专利数: provincePatents}) as provinces

// 获取父级技术领域
OPTIONAL MATCH (parent:TechDomain)-[:HAS_CHILD]->(t)

// 最终返回 - 确保所有变量都可用
WITH t, totalPatents, beijingPatents, highGreenCount, beijingRatio, highGreenRatio, provinces,
     collect(DISTINCT parent.nameZh)[0] as parentName

RETURN 
    t.code as 技术领域代码,
    t.nameZh as 技术领域名称,
    parentName as 所属上级领域,
    totalPatents as 专利总数,
    beijingPatents as 北京专利数,
    round(beijingRatio * 100, 1) as 北京占比百分比,
    round(highGreenRatio * 100, 1) as 高绿色度百分比,
    highGreenCount as 高绿色专利数,
    [prov IN provinces | prov.省份 + '(' + toString(prov.专利数) + ')'][..3] as 主要非北京区域
ORDER BY highGreenRatio DESC, beijingRatio ASC
LIMIT 20
"""

# ============================================================================
# 补充查询：查看绿色技术分类分布（帮助理解为什么GT1数据少）
# ============================================================================

QUERY_GREEN_TECH_STATS = """
// 查看绿色技术分类的整体分布
MATCH (g:GreenTechCategory)<-[:CLASSIFIED_AS_GREEN]-(p:Patent)
RETURN 
    g.code as 绿色分类代码,
    g.categoryType as 绿色分类名称,
    count(p) as 专利数量,
    round(count(p) * 100.0 / 185987, 2) as 占比百分比
ORDER BY 专利数量 DESC
"""

# ============================================================================
# 补充查询：检查地理位置识别情况
# ============================================================================

QUERY_LOCATION_STATS = """
// 统计地理位置识别情况
MATCH (p:Patent)-[:APPLIED_BY]->(e:Entity)
WITH p, e,
     CASE 
       WHEN EXISTS((e)-[:LOCATED_IN_PROVINCE]->()) THEN '已识别'
       WHEN e.address IS NOT NULL AND e.address <> '' THEN '有地址未识别'
       ELSE '无地址'
     END as locationStatus
RETURN 
    locationStatus as 位置状态,
    count(DISTINCT p) as 专利数量,
    round(count(DISTINCT p) * 100.0 / 291989, 2) as 占比百分比
ORDER BY 专利数量 DESC
"""

# ============================================================================
# 辅助查询：查看数据统计概况
# ============================================================================

QUERY_STATISTICS = """
// 数据统计概况
MATCH (p:Patent)
OPTIONAL MATCH (p)-[:BELONGS_TO_TECH]->(t:TechDomain)
OPTIONAL MATCH (p)-[:CLASSIFIED_AS_GREEN]->(g:GreenTechCategory)
OPTIONAL MATCH (p)-[:APPLIED_BY]->(e:Entity)

RETURN 
    count(DISTINCT p) as 专利总数,
    count(DISTINCT CASE WHEN t IS NOT NULL THEN p END) as 已分类技术领域专利数,
    count(DISTINCT CASE WHEN g IS NOT NULL THEN p END) as 已分类绿色类型专利数,
    count(DISTINCT CASE WHEN e.address IS NOT NULL THEN p END) as 有地址信息专利数,
    count(DISTINCT t) as 技术领域数,
    count(DISTINCT g) as 绿色分类数,
    count(DISTINCT e) as 创新主体数
"""

# ============================================================================
# Python使用示例（修复版）
# ============================================================================

if __name__ == "__main__":
    from neo4j import GraphDatabase
    from config import NEO4J_CONFIG

    driver = GraphDatabase.driver(
        NEO4J_CONFIG['uri'],
        auth=(NEO4J_CONFIG['user'], NEO4J_CONFIG['password'])
    )

    print("=" * 80)
    print("氢能知识图谱 Cypher查询示例 - 修复版")
    print("=" * 80)

    with driver.session() as session:
        # 统计概况
        print("\n【数据统计概况】")
        result = session.run(QUERY_STATISTICS)
        for record in result:
            print(f"专利总数: {record['专利总数']:,}")
            print(f"已分类技术领域专利数: {record['已分类技术领域专利数']:,}")
            print(f"已分类绿色类型专利数: {record['已分类绿色类型专利数']:,}")
            print(f"有地址信息专利数: {record['有地址信息专利数']:,}")

        # 绿色技术分布
        print("\n【绿色技术分类分布】")
        result = session.run(QUERY_GREEN_TECH_STATS)
        for record in result:
            print(f"{record['绿色分类名称']}: {record['专利数量']:,} ({record['占比百分比']}%)")

        # 地理位置识别情况
        print("\n【地理位置识别情况】")
        result = session.run(QUERY_LOCATION_STATS)
        for record in result:
            print(f"{record['位置状态']}: {record['专利数量']:,} ({record['占比百分比']}%)")

        # 查询1示例
        print("\n【查询1: 特定技术领域的非北京领先区域】")
        print(f"技术领域: H1.1 (电解水制氢)")
        result = session.run(QUERY_1_NON_BEIJING_LEADERS, QUERY_1_EXAMPLE_PARAMS)
        for i, record in enumerate(result, 1):
            print(f"\n{i}. {record['省份']} - 专利总数: {record['专利总数']}")
            if record['领先创新主体']:
                print("   领先创新主体:")
                for entity in record['领先创新主体'][:3]:
                    print(f"   - {entity['名称']} ({entity['专利数']}项)")
            if i >= 10:  # 只显示前10个
                break

        # 查询2示例
        print("\n【查询2: 特定绿色技术的非北京区域专利分布】")
        print(f"绿色技术类型: GT1 (零碳使能型)")
        result = session.run(QUERY_2_GREEN_TECH_DISTRIBUTION, QUERY_2_EXAMPLE_PARAMS)
        count = 0
        for i, record in enumerate(result, 1):
            print(f"{i}. {record['省份']} - {record['专利总数']}项专利")
            if record['主要技术领域']:
                print(f"   主要技术: {', '.join([t for t in record['主要技术领域'] if t])}")
            count += 1
            if count >= 10:
                break
        if count == 0:
            print("   (无结果 - 可能GT1数据量较少，尝试GT3)")

        # 查询3示例
        print("\n【查询3: 高绿色程度但专利密度低的技术领域】")
        result = session.run(QUERY_3_HIGH_GREEN_LOW_DENSITY)
        count = 0
        for i, record in enumerate(result, 1):
            print(f"{i}. {record['技术领域名称']} ({record['技术领域代码']})")
            print(f"   专利数: {record['专利总数']}, 高绿色度: {record['高绿色度百分比']}%")
            print(f"   GT1: {record['GT1零碳专利数']}, GT2: {record['GT2低碳专利数']}")
            count += 1
            if count >= 10:
                break
        if count == 0:
            print("   (无满足条件的结果 - 可尝试降低绿色度阈值)")

        # 查询4示例
        print("\n【查询4: 北京专利占比低的高绿色技术领域】")
        result = session.run(QUERY_4_LOW_BEIJING_HIGH_GREEN)
        count = 0
        for i, record in enumerate(result, 1):
            print(f"{i}. {record['技术领域名称']}")
            print(f"   北京占比: {record['北京占比百分比']}%, 高绿色度: {record['高绿色度百分比']}%")
            if record['主要非北京区域']:
                print(f"   主要区域: {', '.join(record['主要非北京区域'])}")
            count += 1
            if count >= 10:
                break
        if count == 0:
            print("   (无满足条件的结果 - 可尝试调整阈值)")

    driver.close()
    print("\n" + "=" * 80)