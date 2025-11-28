# -*- coding: utf-8 -*-
"""
氢能专利知识图谱 - 数据导入模块（修改版）
主要功能:
1. 创建所有确定的节点: Patent, Entity, Date, Province, City, District, TechDomain, GreenTechCategory
2. 创建所有确定的边关系（不需要LLM的关系）
3. 批量处理提升性能
4. 地理位置解析(基于规则,LLM会进一步增强)
"""

import os
import re
import pandas as pd
from datetime import datetime
from neo4j import GraphDatabase
import logging
import argparse
from tech_domains import get_all_tech_domains
from config import NEO4J_CONFIG, DATA_CONFIG, LOG_CONFIG, GREEN_TECH_CATEGORIES

# 配置日志
logging.basicConfig(
    level=getattr(logging, LOG_CONFIG['level']),
    format=LOG_CONFIG['format'],
    handlers=[
        logging.FileHandler(LOG_CONFIG['file'], encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class PatentKnowledgeGraphBuilder:
    """专利知识图谱构建器"""

    def __init__(self, uri, user, password, batch_size=5000):
        """初始化Neo4j连接"""
        self.driver = GraphDatabase.driver(
            uri,
            auth=(user, password),
            max_connection_pool_size=100,
            connection_timeout=60
        )
        self.batch_size = batch_size

        # 中国省份、直辖市、自治区列表
        self.provinces = {
            '北京': '北京市', '天津': '天津市', '上海': '上海市', '重庆': '重庆市',
            '河北': '河北省', '山西': '山西省', '辽宁': '辽宁省', '吉林': '吉林省',
            '黑龙江': '黑龙江省', '江苏': '江苏省', '浙江': '浙江省', '安徽': '安徽省',
            '福建': '福建省', '江西': '江西省', '山东': '山东省', '河南': '河南省',
            '湖北': '湖北省', '湖南': '湖南省', '广东': '广东省', '海南': '海南省',
            '四川': '四川省', '贵州': '贵州省', '云南': '云南省', '陕西': '陕西省',
            '甘肃': '甘肃省', '青海': '青海省', '台湾': '台湾省',
            '内蒙古': '内蒙古自治区', '广西': '广西壮族自治区', '西藏': '西藏自治区',
            '宁夏': '宁夏回族自治区', '新疆': '新疆维吾尔自治区',
            '香港': '香港特别行政区', '澳门': '澳门特别行政区'
        }

        logger.info(f"Connected to Neo4j at {uri}")

    def close(self):
        """关闭连接"""
        self.driver.close()
        logger.info("Neo4j connection closed")

    def clear_database(self):
        """清空数据库"""
        logger.warning("Clearing database...")
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
            logger.info("Database cleared successfully")

    def create_constraints(self):
        """创建约束"""
        logger.info("Creating constraints...")
        with self.driver.session() as session:
            constraints = [
                "CREATE CONSTRAINT patent_id IF NOT EXISTS FOR (p:Patent) REQUIRE p.patentId IS UNIQUE",
                "CREATE CONSTRAINT entity_normalized_name IF NOT EXISTS FOR (e:Entity) REQUIRE e.normalizedName IS UNIQUE",
                "CREATE CONSTRAINT techDomain_code IF NOT EXISTS FOR (t:TechDomain) REQUIRE t.code IS UNIQUE",
                "CREATE CONSTRAINT date_value IF NOT EXISTS FOR (d:Date) REQUIRE d.dateValue IS UNIQUE",
                "CREATE CONSTRAINT province_name IF NOT EXISTS FOR (p:Province) REQUIRE p.name IS UNIQUE",
                "CREATE CONSTRAINT city_fullname IF NOT EXISTS FOR (c:City) REQUIRE c.fullName IS UNIQUE",
                "CREATE CONSTRAINT district_fullname IF NOT EXISTS FOR (d:District) REQUIRE d.fullName IS UNIQUE",
                "CREATE CONSTRAINT green_category_code IF NOT EXISTS FOR (g:GreenTechCategory) REQUIRE g.code IS UNIQUE"
            ]

            for constraint in constraints:
                try:
                    session.run(constraint)
                    logger.info(f"✓ {constraint.split('CONSTRAINT')[1].split('IF')[0].strip()}")
                except Exception as e:
                    logger.debug(f"Constraint exists: {e}")

    def create_indexes(self):
        """创建索引(导入完成后)"""
        logger.info("Creating indexes...")
        with self.driver.session() as session:
            indexes = [
                "CREATE INDEX patent_pubNumber IF NOT EXISTS FOR (p:Patent) ON (p.pubNumber)",
                "CREATE INDEX patent_appNumber IF NOT EXISTS FOR (p:Patent) ON (p.appNumber)",
                "CREATE INDEX patent_titleZh IF NOT EXISTS FOR (p:Patent) ON (p.titleZh)",
                "CREATE INDEX entity_type IF NOT EXISTS FOR (e:Entity) ON (e.type)",
                "CREATE INDEX entity_name IF NOT EXISTS FOR (e:Entity) ON (e.name)",
                "CREATE INDEX province_name_idx IF NOT EXISTS FOR (p:Province) ON (p.name)",
                "CREATE INDEX city_name_idx IF NOT EXISTS FOR (c:City) ON (c.name)",
                "CREATE INDEX techDomain_level IF NOT EXISTS FOR (t:TechDomain) ON (t.level)"
            ]

            for index in indexes:
                try:
                    session.run(index)
                    logger.info(f"✓ {index.split('INDEX')[1].split('IF')[0].strip()}")
                except Exception as e:
                    logger.debug(f"Index exists: {e}")

    def create_tech_domains(self):
        """创建技术领域树"""
        logger.info("Creating technology domain tree...")
        domains = get_all_tech_domains()

        with self.driver.session() as session:
            query = """
            UNWIND $domains AS domain
            MERGE (t:TechDomain {code: domain.code})
            SET t.nameZh = domain.nameZh,
                t.nameEn = domain.nameEn,
                t.level = domain.level
            """
            session.run(query, domains=domains)

            # 创建SUBDOMAIN_OF关系
            parent_rels = [{'child_code': d['code'], 'parent_code': d['parent_code']}
                           for d in domains if d['parent_code']]

            if parent_rels:
                query = """
                UNWIND $rels AS rel
                MATCH (child:TechDomain {code: rel.child_code})
                MATCH (parent:TechDomain {code: rel.parent_code})
                MERGE (child)-[:SUBDOMAIN_OF]->(parent)
                """
                session.run(query, rels=parent_rels)

            logger.info(f"✓ Created {len(domains)} technology domains")

    def create_green_tech_categories(self):
        """创建绿色技术分类节点"""
        logger.info("Creating green technology categories...")

        categories_list = []
        for name, data in GREEN_TECH_CATEGORIES.items():
            categories_list.append({
                'code': data['code'],
                'categoryType': data['categoryType'],
                'definition': data['definition'],
                'typicalExamples': data['typicalExamples']
            })

        with self.driver.session() as session:
            query = """
            UNWIND $categories AS cat
            MERGE (g:GreenTechCategory {code: cat.code})
            SET g.categoryType = cat.categoryType,
                g.definition = cat.definition,
                g.typicalExamples = cat.typicalExamples
            """
            session.run(query, categories=categories_list)
            logger.info(f"✓ Created {len(categories_list)} green technology categories")

    def extract_location_from_entity(self, entity_name):
        """
        从实体名称中提取地理位置信息 (基于规则)
        返回: {'province': 'XX', 'city': 'XX', 'district': 'XX'}
        """
        if not entity_name or len(entity_name) < 2:
            return None

        result = {'province': None, 'city': None, 'district': None}

        # 检查省份
        for short_name, full_name in self.provinces.items():
            if short_name in entity_name:
                result['province'] = full_name

                # 尝试提取市级信息
                # 修复：将 {{2,8}} 改为 {2,8}（移除多余的大括号）
                city_pattern = rf'{short_name}[省市]?([^省市县区]{2,8})市'
                city_match = re.search(city_pattern, entity_name)
                if city_match:
                    result['city'] = city_match.group(1) + '市'

                    # 尝试提取区县信息
                    # 修复：将 {{2,6}} 改为 {2,6}（移除多余的大括号）
                    district_pattern = rf"{city_match.group(1)}市([^市县区]{2,6})[县区]"
                    district_match = re.search(district_pattern, entity_name)
                    if district_match:
                        result['district'] = district_match.group(1)
                        # 判断是县还是区
                        if '县' in entity_name[district_match.end() - 1:district_match.end() + 1]:
                            result['district'] += '县'
                        else:
                            result['district'] += '区'

                break

        # 如果找到了至少省份信息,返回结果
        if result['province']:
            return result

        return None

    def normalize_patent_number(self, number_str):
        """规范化专利号"""
        if pd.isna(number_str) or not number_str:
            return None
        normalized = str(number_str).strip().replace(' ', '').replace('\n', '')
        return normalized if normalized else None

    def normalize_entity_name(self, name_str):
        """规范化实体名称"""
        if pd.isna(name_str) or not name_str:
            return None

        name = str(name_str).strip()
        if len(name) < 2:
            return None

        # 移除常见公司后缀
        common_suffixes = [
            '有限责任公司', '股份有限公司', '有限公司',
            'Co.,Ltd.', 'Co., Ltd.', 'Co.,Ltd', 'Co., Ltd',
            'Ltd.', 'Ltd', 'Inc.', 'Inc', 'Corp.', 'Corp',
            'LIMITED', 'CORPORATION', 'INCORPORATED'
        ]

        original_name = name
        for suffix in common_suffixes:
            if name.endswith(suffix):
                name = name[:-len(suffix)].strip()
                break

        if not name or len(name) < 2:
            return original_name

        # 移除多余的空格和标点
        name = re.sub(r'\s+', '', name)
        name = re.sub(r'[（）\(\)【】\[\]《》<>]', '', name)

        return name if name and len(name) >= 2 else original_name

    def parse_date(self, date_str):
        """解析日期为YYYY-MM-DD格式"""
        if pd.isna(date_str) or not date_str:
            return None

        date_str = str(date_str).strip()
        formats = ['%Y-%m-%d', '%Y/%m/%d', '%Y.%m.%d', '%Y%m%d',
                   '%Y-%m-%d %H:%M:%S', '%Y/%m/%d %H:%M:%S']

        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt).strftime('%Y-%m-%d')
            except ValueError:
                continue
        return None

    def split_entities(self, entity_str):
        """分割实体字符串"""
        if pd.isna(entity_str) or not entity_str:
            return []
        entity_str = str(entity_str).strip()
        entities = re.split(r'[;,、；]', entity_str)
        return [e.strip() for e in entities if e.strip() and len(e.strip()) >= 2]

    def process_file_data(self, file_path, tech_domain_code):
        """处理单个文件,返回结构化数据"""
        logger.info(f"Processing: {os.path.basename(file_path)}")

        df = pd.read_excel(file_path)
        patents_list = []
        stats = {'total': 0, 'valid': 0, 'skipped': 0}

        for idx, row in df.iterrows():
            stats['total'] += 1

            pub_number = self.normalize_patent_number(row.get('公开(公告)号'))
            app_number = self.normalize_patent_number(row.get('申请号'))

            if not pub_number and not app_number:
                stats['skipped'] += 1
                continue

            patent_id = pub_number if pub_number else app_number

            patent_data = {
                'patentId': patent_id,
                'pubNumber': pub_number,
                'appNumber': app_number,
                'titleZh': str(row.get('标题 (中文)', '')).strip() or None,
                'titleEn': str(row.get('标题 (英文)', '')).strip() or None,
                'abstractZh': str(row.get('摘要 (中文)', '')).strip() or None,
                'abstractEn': str(row.get('摘要 (英文)', '')).strip() or None,
                'patentType': str(row.get('专利类型', '')).strip() or None,
                'legalStatus': str(row.get('当前法律状态', '')).strip() or None,
                'ipcMainClass': str(row.get('IPC主分类', '')).strip() or None,
                'publicCountry': str(row.get('公开国别', '')).strip() or None,
                'techDomainCode': tech_domain_code,

                'applicants': self.split_entities(row.get('申请人')),
                'currentOwners': self.split_entities(row.get('当前权利人')),
                'appDate': self.parse_date(row.get('申请日')),
                'pubDate': self.parse_date(
                    row.get('公开(公告)日\n(如专利类型为"发明授权"或"实用新型"或"外观设计"的,此项为授权公告日)')),
                'familyPatents': self.split_entities(row.get('简单同族')),
                'assignors': self.split_entities(row.get('转让人')),
                'assignees': self.split_entities(row.get('受让人')),
                'licensors': self.split_entities(row.get('许可人')),
                'currentLicensees': self.split_entities(row.get('当前被许可人')),
                'pledgors': self.split_entities(row.get('出质人')),
                'pledgees': self.split_entities(row.get('质权人')),
                'plaintiffs': self.split_entities(row.get('原告')),
                'defendants': self.split_entities(row.get('被告'))
            }

            patents_list.append(patent_data)
            stats['valid'] += 1

        logger.info(f"✓ Processed {os.path.basename(file_path)}: "
                    f"{stats['valid']}/{stats['total']} valid patents, "
                    f"{stats['skipped']} skipped")
        return patents_list

    def batch_create_all_nodes_and_relationships(self, patents_batch):
        """批量创建所有节点和关系"""
        with self.driver.session() as session:
            # 准备批量数据
            batch_data = []

            for patent in patents_batch:
                item = {
                    'patent': {
                        'patentId': patent['patentId'],
                        'pubNumber': patent['pubNumber'],
                        'appNumber': patent['appNumber'],
                        'titleZh': patent['titleZh'],
                        'titleEn': patent['titleEn'],
                        'abstractZh': patent['abstractZh'],
                        'abstractEn': patent['abstractEn'],
                        'patentType': patent['patentType'],
                        'legalStatus': patent['legalStatus'],
                        'ipcMainClass': patent['ipcMainClass'],
                        'publicCountry': patent['publicCountry'],
                        'techDomainCode': patent['techDomainCode']
                    },
                    'dates': [],
                    'entities': [],
                    'familyPatents': patent.get('familyPatents', [])
                }

                # 添加日期信息
                if patent['appDate']:
                    item['dates'].append({
                        'dateValue': patent['appDate'],
                        'dateType': '申请日',
                        'relType': 'FILED_ON'
                    })
                if patent['pubDate']:
                    item['dates'].append({
                        'dateValue': patent['pubDate'],
                        'dateType': '公开公告日',
                        'relType': 'PUBLISHED_ON'
                    })

                # 添加实体信息
                for applicant in patent.get('applicants', []):
                    normalized = self.normalize_entity_name(applicant)
                    if normalized:
                        location = self.extract_location_from_entity(applicant)
                        item['entities'].append({
                            'originalName': applicant,
                            'normalizedName': normalized,
                            'relType': 'APPLIED_BY',
                            'location': location
                        })

                for owner in patent.get('currentOwners', []):
                    normalized = self.normalize_entity_name(owner)
                    if normalized:
                        location = self.extract_location_from_entity(owner)
                        item['entities'].append({
                            'originalName': owner,
                            'normalizedName': normalized,
                            'relType': 'OWNED_BY',
                            'location': location
                        })

                for assignor in patent.get('assignors', []):
                    normalized = self.normalize_entity_name(assignor)
                    if normalized:
                        item['entities'].append({
                            'originalName': assignor,
                            'normalizedName': normalized,
                            'relType': 'ASSIGNED_FROM',
                            'location': None
                        })

                for assignee in patent.get('assignees', []):
                    normalized = self.normalize_entity_name(assignee)
                    if normalized:
                        item['entities'].append({
                            'originalName': assignee,
                            'normalizedName': normalized,
                            'relType': 'ASSIGNED_TO',
                            'location': None
                        })

                for licensee in patent.get('currentLicensees', []):
                    normalized = self.normalize_entity_name(licensee)
                    if normalized:
                        item['entities'].append({
                            'originalName': licensee,
                            'normalizedName': normalized,
                            'relType': 'LICENSED_TO',
                            'location': None
                        })

                for pledgor in patent.get('pledgors', []):
                    normalized = self.normalize_entity_name(pledgor)
                    if normalized:
                        item['entities'].append({
                            'originalName': pledgor,
                            'normalizedName': normalized,
                            'relType': 'PLEDGED_BY',
                            'location': None
                        })

                for pledgee in patent.get('pledgees', []):
                    normalized = self.normalize_entity_name(pledgee)
                    if normalized:
                        item['entities'].append({
                            'originalName': pledgee,
                            'normalizedName': normalized,
                            'relType': 'PLEDGED_TO',
                            'location': None
                        })

                for plaintiff in patent.get('plaintiffs', []):
                    normalized = self.normalize_entity_name(plaintiff)
                    if normalized:
                        item['entities'].append({
                            'originalName': plaintiff,
                            'normalizedName': normalized,
                            'relType': 'PLAINTIFF_IN',
                            'location': None
                        })

                for defendant in patent.get('defendants', []):
                    normalized = self.normalize_entity_name(defendant)
                    if normalized:
                        item['entities'].append({
                            'originalName': defendant,
                            'normalizedName': normalized,
                            'relType': 'DEFENDANT_IN',
                            'location': None
                        })

                batch_data.append(item)

            # ========== 批量创建专利节点 ==========
            session.run("""
                UNWIND $batch AS item
                MERGE (p:Patent {patentId: item.patent.patentId})
                SET p.pubNumber = item.patent.pubNumber,
                    p.appNumber = item.patent.appNumber,
                    p.titleZh = item.patent.titleZh,
                    p.titleEn = item.patent.titleEn,
                    p.abstractZh = item.patent.abstractZh,
                    p.abstractEn = item.patent.abstractEn,
                    p.patentType = item.patent.patentType,
                    p.legalStatus = item.patent.legalStatus,
                    p.ipcMainClass = item.patent.ipcMainClass,
                    p.publicCountry = item.patent.publicCountry
            """, batch=batch_data)

            # ========== 创建专利-技术领域关系(BELONGS_TO_TECH) ==========
            # 注意：这是初步分类，LLM会进一步细化到三级分类
            tech_rels = [{
                'patentId': item['patent']['patentId'],
                'techCode': item['patent']['techDomainCode']
            } for item in batch_data if item['patent']['techDomainCode']]

            if tech_rels:
                session.run("""
                    UNWIND $batch AS item
                    MATCH (p:Patent {patentId: item.patentId})
                    MATCH (t:TechDomain {code: item.techCode})
                    MERGE (p)-[:BELONGS_TO_TECH {source: 'initial'}]->(t)
                """, batch=tech_rels)

            # ========== 批量创建日期节点和关系 ==========
            date_batch = []
            for item in batch_data:
                for date_info in item['dates']:
                    date_batch.append({
                        'patentId': item['patent']['patentId'],
                        'dateValue': date_info['dateValue'],
                        'dateType': date_info['dateType'],
                        'relType': date_info['relType']
                    })

            if date_batch:
                # 先创建日期节点
                session.run("""
                    UNWIND $batch AS item
                    MERGE (d:Date {dateValue: item.dateValue})
                    SET d.dateType = item.dateType
                """, batch=date_batch)

                # 批量创建申请日关系
                filed_dates = [d for d in date_batch if d['relType'] == 'FILED_ON']
                if filed_dates:
                    session.run("""
                        UNWIND $batch AS item
                        MATCH (p:Patent {patentId: item.patentId})
                        MATCH (d:Date {dateValue: item.dateValue})
                        MERGE (p)-[:FILED_ON]->(d)
                    """, batch=filed_dates)

                # 批量创建公开日关系
                pub_dates = [d for d in date_batch if d['relType'] == 'PUBLISHED_ON']
                if pub_dates:
                    session.run("""
                        UNWIND $batch AS item
                        MATCH (p:Patent {patentId: item.patentId})
                        MATCH (d:Date {dateValue: item.dateValue})
                        MERGE (p)-[:PUBLISHED_ON]->(d)
                    """, batch=pub_dates)

            # ========== 批量创建实体节点 ==========
            entity_batch = []
            for item in batch_data:
                for entity_info in item['entities']:
                    entity_batch.append({
                        'normalizedName': entity_info['normalizedName'],
                        'originalName': entity_info['originalName']
                    })

            if entity_batch:
                # 去重
                unique_entities = {e['normalizedName']: e for e in entity_batch}.values()
                session.run("""
                    UNWIND $batch AS item
                    MERGE (e:Entity {normalizedName: item.normalizedName})
                    SET e.name = item.originalName,
                        e.type = CASE
                            WHEN item.originalName =~ '.*公司.*|.*Corp.*|.*Ltd.*|.*Inc.*' THEN '企业'
                            ELSE '个人/其他'
                        END
                """, batch=list(unique_entities))

            # ========== 批量创建专利-实体关系 ==========
            # 按关系类型分组
            rel_by_type = {}
            for item in batch_data:
                for entity_info in item['entities']:
                    rel_type = entity_info['relType']
                    if rel_type not in rel_by_type:
                        rel_by_type[rel_type] = []
                    rel_by_type[rel_type].append({
                        'patentId': item['patent']['patentId'],
                        'normalizedName': entity_info['normalizedName']
                    })

            # 批量创建每种类型的关系
            for rel_type, rels in rel_by_type.items():
                if rels:
                    session.run(f"""
                        UNWIND $batch AS item
                        MATCH (p:Patent {{patentId: item.patentId}})
                        MATCH (e:Entity {{normalizedName: item.normalizedName}})
                        MERGE (p)-[:{rel_type}]->(e)
                    """, batch=rels)

            # ========== 批量创建地理节点和关系 ==========
            province_batch = []
            city_batch = []
            district_batch = []

            for item in batch_data:
                for entity_info in item['entities']:
                    if entity_info['location']:
                        loc = entity_info['location']

                        # 收集省份数据
                        if loc['province']:
                            province_batch.append({
                                'normalizedName': entity_info['normalizedName'],
                                'provinceName': loc['province']
                            })

                        # 收集城市数据
                        if loc['city'] and loc['province']:
                            city_batch.append({
                                'normalizedName': entity_info['normalizedName'],
                                'fullName': f"{loc['province']}-{loc['city']}",
                                'cityName': loc['city'],
                                'provinceName': loc['province']
                            })

                        # 收集区县数据
                        if loc['district'] and loc['city'] and loc['province']:
                            district_batch.append({
                                'normalizedName': entity_info['normalizedName'],
                                'fullName': f"{loc['province']}-{loc['city']}-{loc['district']}",
                                'districtName': loc['district'],
                                'cityName': loc['city'],
                                'provinceName': loc['province']
                            })

            # 批量创建省份节点和关系
            if province_batch:
                session.run("""
                    UNWIND $batch AS item
                    MERGE (p:Province {name: item.provinceName})
                """, batch=province_batch)

                session.run("""
                    UNWIND $batch AS item
                    MATCH (e:Entity {normalizedName: item.normalizedName})
                    MATCH (p:Province {name: item.provinceName})
                    MERGE (e)-[:LOCATED_IN_PROVINCE]->(p)
                """, batch=province_batch)

            # 批量创建城市节点和关系
            if city_batch:
                session.run("""
                    UNWIND $batch AS item
                    MERGE (c:City {fullName: item.fullName})
                    SET c.name = item.cityName,
                        c.province = item.provinceName
                """, batch=city_batch)

                session.run("""
                    UNWIND $batch AS item
                    MATCH (e:Entity {normalizedName: item.normalizedName})
                    MATCH (c:City {fullName: item.fullName})
                    MERGE (e)-[:LOCATED_IN_CITY]->(c)
                """, batch=city_batch)

            # 批量创建区县节点和关系
            if district_batch:
                session.run("""
                    UNWIND $batch AS item
                    MERGE (d:District {fullName: item.fullName})
                    SET d.name = item.districtName,
                        d.city = item.cityName,
                        d.province = item.provinceName
                """, batch=district_batch)

                session.run("""
                    UNWIND $batch AS item
                    MATCH (e:Entity {normalizedName: item.normalizedName})
                    MATCH (d:District {fullName: item.fullName})
                    MERGE (e)-[:LOCATED_IN_DISTRICT]->(d)
                """, batch=district_batch)

            # ========== 批量创建同族专利关系 ==========
            family_batch = []
            for item in batch_data:
                if item['familyPatents']:
                    for family_patent in item['familyPatents']:
                        family_patent_normalized = self.normalize_patent_number(family_patent)
                        if family_patent_normalized:
                            family_batch.append({
                                'patent1': item['patent']['patentId'],
                                'patent2': family_patent_normalized
                            })

            if family_batch:
                session.run("""
                    UNWIND $batch AS item
                    MATCH (p1:Patent {patentId: item.patent1})
                    MERGE (p2:Patent {patentId: item.patent2})
                    MERGE (p1)-[:HAS_FAMILY]-(p2)
                """, batch=family_batch)

    def import_all_data(self):
        """导入所有数据"""
        logger.info("Starting data import...")

        data_dir = DATA_CONFIG['data_dir']
        file_mapping = DATA_CONFIG['file_mapping']

        all_patents = []

        # 处理所有文件
        for filename, tech_code in file_mapping.items():
            file_path = os.path.join(data_dir, filename)
            if os.path.exists(file_path):
                patents = self.process_file_data(file_path, tech_code)
                all_patents.extend(patents)
            else:
                logger.warning(f"File not found: {file_path}")

        logger.info(f"Total patents to import: {len(all_patents)}")

        # 分批导入
        total_batches = (len(all_patents) + self.batch_size - 1) // self.batch_size

        for i in range(0, len(all_patents), self.batch_size):
            batch = all_patents[i:i + self.batch_size]
            batch_num = i // self.batch_size + 1

            logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} patents)...")
            self.batch_create_all_nodes_and_relationships(batch)

        logger.info(f"✓ Imported {len(all_patents)} patents successfully")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='Import patent data into Neo4j knowledge graph')
    parser.add_argument('--clear', action='store_true', help='Clear database before import')
    args = parser.parse_args()

    builder = PatentKnowledgeGraphBuilder(
        NEO4J_CONFIG['uri'],
        NEO4J_CONFIG['user'],
        NEO4J_CONFIG['password'],
        batch_size=5000
    )

    try:
        if args.clear:
            builder.clear_database()

        logger.info("=" * 60)
        logger.info("Starting Knowledge Graph Construction")
        logger.info("=" * 60)

        # Step 1: 创建约束
        builder.create_constraints()

        # Step 2: 创建技术领域树
        builder.create_tech_domains()

        # Step 3: 创建绿色技术分类节点
        builder.create_green_tech_categories()

        # Step 4: 导入专利数据
        start_time = datetime.now()
        builder.import_all_data()
        end_time = datetime.now()

        logger.info(f"Import time: {(end_time - start_time).total_seconds():.2f} seconds")

        # Step 5: 创建索引
        builder.create_indexes()

        logger.info("=" * 60)
        logger.info("✓ Knowledge Graph Construction Completed!")
        logger.info("✓ Next step: run llm_generate_json.py for LLM-based enhancements")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        raise
    finally:
        builder.close()


if __name__ == "__main__":
    main()