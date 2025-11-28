# -*- coding: utf-8 -*-
"""
LLM数据生成模块 - 直接从Excel读取数据（无需Neo4j）
功能：
1. 从本地Excel文件读取专利数据
2. 调用LLM生成分类结果并保存到JSON文件
3. 支持断点续传
4. 支持Ctrl+C安全退出
5. 异步并发处理，大幅提升速度
"""

import asyncio
import aiohttp
import json
import os
import signal
import time
import threading
import pandas as pd
import re
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
import logging

from config import DATA_CONFIG, LLM_CONFIG, LLM_ENHANCE_CONFIG, GREEN_TECH_CATEGORIES
from tech_domains import get_tech_tree_text

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('llm_generate_json.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class JSONStorage:
    """JSON文件存储管理器 - 支持断点续传和实时保存"""

    def __init__(self, output_dir: str, session_id: str):
        self.output_dir = output_dir
        self.session_id = session_id
        os.makedirs(output_dir, exist_ok=True)

        # 各类数据的JSON文件路径
        self.files = {
            'green_classifications': os.path.join(output_dir, f'{session_id}_green_classifications.json'),
            'tech_classifications': os.path.join(output_dir, f'{session_id}_tech_classifications.json'),
            'entity_locations': os.path.join(output_dir, f'{session_id}_entity_locations.json'),
            'progress': os.path.join(output_dir, f'{session_id}_progress.json')
        }

        self.lock = threading.Lock()

        # 加载已有数据
        self.data = {
            'green_classifications': self._load_json('green_classifications'),
            'tech_classifications': self._load_json('tech_classifications'),
            'entity_locations': self._load_json('entity_locations'),
            'progress': self._load_json('progress') or {'processed_patents': [], 'processed_entities': [],
                                                        'session_id': session_id}
        }

    def _load_json(self, data_type: str):
        """加载JSON文件"""
        filepath = self.files[data_type]
        if os.path.exists(filepath):
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if data_type == 'progress':
                        logger.info(f"✓ 已加载进度: {len(data.get('processed_patents', []))} 个专利已处理")
                    else:
                        logger.info(f"✓ 已加载 {data_type}: {len(data) if isinstance(data, list) else 'N/A'} 条记录")
                    return data
            except Exception as e:
                logger.warning(f"加载 {data_type} 失败: {e}")
                if data_type == 'progress':
                    return {'processed_patents': [], 'processed_entities': [], 'session_id': self.session_id}
                return []
        return [] if data_type != 'progress' else {'processed_patents': [], 'processed_entities': [],
                                                   'session_id': self.session_id}

    def _save_json(self, data_type: str):
        """保存JSON文件 - 使用原子操作"""
        filepath = self.files[data_type]
        try:
            # 先写入临时文件,再重命名(原子操作,防止写入过程中断导致文件损坏)
            temp_filepath = filepath + '.tmp'
            with open(temp_filepath, 'w', encoding='utf-8') as f:
                json.dump(self.data[data_type], f, ensure_ascii=False, indent=2)

            # 重命名
            if os.path.exists(filepath):
                os.remove(filepath)
            os.rename(temp_filepath, filepath)

        except Exception as e:
            logger.error(f"保存 {data_type} 失败: {e}")

    def append_data(self, data_type: str, items: List[Dict]):
        """追加数据到JSON文件并立即保存"""
        with self.lock:
            if data_type == 'progress':
                # progress特殊处理
                if 'patents' in str(items):
                    self.data[data_type]['processed_patents'].extend(items)
                elif 'entities' in str(items):
                    self.data[data_type]['processed_entities'].extend(items)
            else:
                self.data[data_type].extend(items)
            self._save_json(data_type)

    def mark_processed_patents(self, patent_ids: List[str]):
        """标记专利为已处理并立即保存"""
        with self.lock:
            self.data['progress']['processed_patents'].extend(patent_ids)
            self.data['progress']['last_update'] = datetime.now().isoformat()
            self._save_json('progress')

    def mark_processed_entities(self, entity_names: List[str]):
        """标记实体为已处理并立即保存"""
        with self.lock:
            self.data['progress']['processed_entities'].extend(entity_names)
            self.data['progress']['last_update'] = datetime.now().isoformat()
            self._save_json('progress')

    def is_patent_processed(self, patent_id: str) -> bool:
        """检查专利是否已处理"""
        return patent_id in self.data['progress']['processed_patents']

    def is_entity_processed(self, entity_name: str) -> bool:
        """检查实体是否已处理"""
        return entity_name in self.data['progress']['processed_entities']

    def get_processed_count(self) -> Dict:
        """获取已处理的数量"""
        return {
            'patents': len(self.data['progress']['processed_patents']),
            'entities': len(self.data['progress']['processed_entities'])
        }

    def get_stats(self) -> Dict:
        """获取统计信息"""
        return {
            'session_id': self.session_id,
            'processed_patents': len(self.data['progress']['processed_patents']),
            'processed_entities': len(self.data['progress']['processed_entities']),
            'green_classifications': len(self.data['green_classifications']),
            'tech_classifications': len(self.data['tech_classifications']),
            'entity_locations': len(self.data['entity_locations'])
        }


class AsyncLLMGenerator:
    """异步LLM数据生成器 - 直接从Excel读取数据"""

    def __init__(self, session_id: Optional[str] = None):
        """初始化"""
        # 会话ID
        if session_id:
            self.session_id = session_id
            logger.info(f"恢复会话: {self.session_id}")
        else:
            self.session_id = datetime.now().strftime('%Y%m%d_%H%M%S')
            logger.info(f"新建会话: {self.session_id}")

        # JSON存储
        self.storage = JSONStorage(LLM_ENHANCE_CONFIG['output_dir'], self.session_id)

        # 技术树文本
        self.tech_tree_text = get_tech_tree_text()

        # 并发控制
        self.max_concurrent = LLM_ENHANCE_CONFIG['max_concurrent_requests']
        self.semaphore = None  # 异步初始化

        # 信号处理标志
        self.should_stop = False
        self._setup_signal_handlers()

        # 统计信息
        self.stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'retries': 0,
            'total_time': 0
        }

        # 从Excel加载所有专利数据
        self.all_patents = self._load_patents_from_excel()
        self.all_entities = self._extract_entities_from_patents()

        logger.info(f"✓ 从Excel加载了 {len(self.all_patents)} 个专利")
        logger.info(f"✓ 提取了 {len(self.all_entities)} 个实体")

    def _setup_signal_handlers(self):
        """设置信号处理器"""

        def signal_handler(signum, frame):
            logger.info("\n⚠️ 接收到中断信号，正在安全退出...")
            self.should_stop = True

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    def _normalize_patent_number(self, number_str):
        """规范化专利号"""
        if pd.isna(number_str) or not number_str:
            return None
        normalized = str(number_str).strip().replace(' ', '').replace('\n', '')
        return normalized if normalized else None

    def _split_entities(self, entity_str):
        """分割实体字符串"""
        if pd.isna(entity_str) or not entity_str:
            return []
        entity_str = str(entity_str).strip()
        entities = re.split(r'[;,、；]', entity_str)
        return [e.strip() for e in entities if e.strip() and len(e.strip()) >= 2]

    def _normalize_entity_name(self, name_str):
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

    def _load_patents_from_excel(self) -> List[Dict]:
        """从Excel文件加载所有专利数据"""
        logger.info("正在从Excel文件加载专利数据...")

        data_dir = DATA_CONFIG['data_dir']
        file_mapping = DATA_CONFIG['file_mapping']

        all_patents = []

        for filename, tech_code in file_mapping.items():
            file_path = os.path.join(data_dir, filename)

            if not os.path.exists(file_path):
                logger.warning(f"文件不存在: {file_path}")
                continue

            try:
                df = pd.read_excel(file_path)
                logger.info(f"读取文件: {filename}, 共 {len(df)} 条记录")

                for idx, row in df.iterrows():
                    pub_number = self._normalize_patent_number(row.get('公开(公告)号'))
                    app_number = self._normalize_patent_number(row.get('申请号'))

                    if not pub_number and not app_number:
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
                        'ipcMainClass': str(row.get('IPC主分类', '')).strip() or None,
                        'techDomainCode': tech_code,
                        'applicants': self._split_entities(row.get('申请人')),
                        'currentOwners': self._split_entities(row.get('当前权利人')),
                    }

                    # 只保留有标题和摘要的专利
                    if patent_data['titleZh'] and patent_data['abstractZh']:
                        all_patents.append(patent_data)

            except Exception as e:
                logger.error(f"读取文件 {filename} 失败: {e}")

        return all_patents

    def _extract_entities_from_patents(self) -> List[Dict]:
        """从专利数据中提取所有实体"""
        logger.info("正在从专利数据中提取实体...")

        entities_dict = {}  # 使用字典去重

        for patent in self.all_patents:
            # 提取申请人
            for applicant in patent.get('applicants', []):
                normalized = self._normalize_entity_name(applicant)
                if normalized and normalized not in entities_dict:
                    entities_dict[normalized] = {
                        'normalizedName': normalized,
                        'name': applicant,
                        'type': '企业' if any(x in applicant for x in ['公司', 'Corp', 'Ltd', 'Inc']) else '个人/其他'
                    }

            # 提取当前权利人
            for owner in patent.get('currentOwners', []):
                normalized = self._normalize_entity_name(owner)
                if normalized and normalized not in entities_dict:
                    entities_dict[normalized] = {
                        'normalizedName': normalized,
                        'name': owner,
                        'type': '企业' if any(x in owner for x in ['公司', 'Corp', 'Ltd', 'Inc']) else '个人/其他'
                    }

        return list(entities_dict.values())

    def get_unprocessed_patents(self, limit: int = 100) -> List[Dict]:
        """获取未处理的专利"""
        unprocessed = []
        for patent in self.all_patents:
            if not self.storage.is_patent_processed(patent['patentId']):
                unprocessed.append(patent)
                if len(unprocessed) >= limit:
                    break
        return unprocessed

    def get_unprocessed_entities(self, limit: int = 100) -> List[Dict]:
        """获取未处理的实体"""
        unprocessed = []
        for entity in self.all_entities:
            if not self.storage.is_entity_processed(entity['normalizedName']):
                unprocessed.append(entity)
                if len(unprocessed) >= limit:
                    break
        return unprocessed

    def get_total_patents_count(self) -> int:
        """获取专利总数"""
        return len(self.all_patents)

    def get_total_entities_count(self) -> int:
        """获取实体总数"""
        return len(self.all_entities)

    async def create_session(self) -> aiohttp.ClientSession:
        """创建带连接池的HTTP会话"""
        timeout = aiohttp.ClientTimeout(total=LLM_ENHANCE_CONFIG['request_timeout'])
        connector = aiohttp.TCPConnector(
            limit=LLM_ENHANCE_CONFIG['connection_limit'],
            limit_per_host=50,
            ttl_dns_cache=300
        )
        return aiohttp.ClientSession(timeout=timeout, connector=connector)

    async def call_llm_async(
            self,
            session: aiohttp.ClientSession,
            prompt: str,
            retry_count: int = 0
    ) -> Optional[str]:
        """异步调用LLM API（带重试机制）"""

        async with self.semaphore:  # 控制并发数
            url = f"{LLM_CONFIG['api_base']}/chat/completions"
            headers = {
                "Authorization": f"Bearer {LLM_CONFIG['api_key']}",
                "Content-Type": "application/json"
            }
            payload = {
                "model": LLM_CONFIG['model'],
                "messages": [
                    {"role": "system", "content": "你是专利分析专家。仔细分析输入,返回准确的JSON格式结果。"},
                    {"role": "user", "content": prompt}
                ],
                "temperature": LLM_CONFIG['temperature'],
                "max_tokens": LLM_CONFIG['max_tokens']
            }

            try:
                self.stats['total_requests'] += 1
                start_time = time.time()

                async with session.post(url, json=payload, headers=headers) as response:
                    elapsed = time.time() - start_time
                    self.stats['total_time'] += elapsed

                    if response.status == 200:
                        result = await response.json()
                        self.stats['successful_requests'] += 1
                        return result['choices'][0]['message']['content'].strip()
                    else:
                        error_text = await response.text()
                        logger.error(f"API请求失败 (状态码: {response.status}): {error_text}")

                        # 重试逻辑
                        if retry_count < LLM_ENHANCE_CONFIG['max_retries']:
                            self.stats['retries'] += 1
                            await asyncio.sleep(LLM_ENHANCE_CONFIG['retry_delay'] * (retry_count + 1))
                            return await self.call_llm_async(session, prompt, retry_count + 1)

                        self.stats['failed_requests'] += 1
                        return None

            except asyncio.TimeoutError:
                logger.error(f"请求超时 (重试 {retry_count})")
                if retry_count < LLM_ENHANCE_CONFIG['max_retries']:
                    self.stats['retries'] += 1
                    await asyncio.sleep(LLM_ENHANCE_CONFIG['retry_delay'] * (retry_count + 1))
                    return await self.call_llm_async(session, prompt, retry_count + 1)
                self.stats['failed_requests'] += 1
                return None

            except Exception as e:
                logger.error(f"LLM调用异常: {e}")
                if retry_count < LLM_ENHANCE_CONFIG['max_retries']:
                    self.stats['retries'] += 1
                    await asyncio.sleep(LLM_ENHANCE_CONFIG['retry_delay'] * (retry_count + 1))
                    return await self.call_llm_async(session, prompt, retry_count + 1)
                self.stats['failed_requests'] += 1
                return None

    def build_green_classification_prompt(self, patents: List[Dict]) -> str:
        """构建绿色技术分类提示词"""
        categories_desc = []
        for name, data in GREEN_TECH_CATEGORIES.items():
            examples_str = "\n      ".join([f"• {ex}" for ex in data['typicalExamples']])
            categories_desc.append(
                f"**{data['categoryType']}** (代码: {data['code']})\n"
                f"  定义: {data['definition']}\n"
                f"  典型示例:\n      {examples_str}"
            )

        categories_text = "\n\n".join(categories_desc)

        patents_info = []
        for i, p in enumerate(patents):
            patents_info.append(
                f"### 专利{i + 1}\n"
                f"ID: {p['patentId']}\n"
                f"标题: {p['titleZh']}\n"
                f"摘要: {p['abstractZh'][:300]}...\n"
                f"IPC: {p.get('ipcMainClass', '未知')}"
            )

        prompt = f"""请对以下氢能专利进行绿色技术五分类。

{chr(10).join(patents_info)}

## 分类标准

{categories_text}

## 分析要点

1. 识别核心技术路径(电解制氢/化石能源制氢/储运/应用)
2. 判断能源来源依赖(可再生能源/化石能源/通用)
3. 评估碳排放特征(零碳/低碳/高碳/中性)
4. 确定是否锁定特定路径
5. 综合判断所属类别

## 输出格式

返回JSON数组,每个专利一个对象:
```json
[
  {{
    "patent_id": "专利ID",
    "category_code": "GT1",
    "category_type": "零碳使能型",
    "confidence": 0.9,
    "reasoning": "详细的分类理由"
  }}
]
```

只返回JSON,不要其他内容。"""

        return prompt

    def build_tech_classification_prompt(self, patents: List[Dict]) -> str:
        """构建技术领域分类提示词"""
        patents_info = []
        for i, p in enumerate(patents):
            patents_info.append(
                f"### 专利{i + 1}\n"
                f"ID: {p['patentId']}\n"
                f"标题: {p['titleZh']}\n"
                f"摘要: {p['abstractZh'][:300]}...\n"
                f"IPC: {p.get('ipcMainClass', '未知')}"
            )

        prompt = f"""请为以下氢能专利识别其所属的技术领域。

{chr(10).join(patents_info)}

## 技术领域分类体系

{self.tech_tree_text}

## 分类原则

1. **优先三级分类**：尽可能识别到L3（三级）具体技术
2. **二级分类作为备选**：如果无法明确到L3，则分类到L2（二级）
3. **一级分类仅作兜底**：只有在完全无法判断具体技术时，才使用L1
4. **H4为其他类别**：如果专利明确不属于制、储运、用任何一类，归入H4
5. **可多分类**：一个专利可以属于多个技术领域

## 输出格式

返回JSON数组,每个专利一个对象:
```json
[
  {{
    "patent_id": "专利ID",
    "tech_domains": [
      {{
        "code": "H1.1.2",
        "level": 3,
        "confidence": 0.95,
        "reasoning": "专利描述了质子交换膜电解槽的优化技术"
      }}
    ]
  }}
]
```

只返回JSON,不要其他内容。"""

        return prompt

    def build_location_extraction_prompt(self, entities: List[Dict]) -> str:
        """构建地理位置提取提示词"""
        entities_info = []
        for i, e in enumerate(entities):
            entities_info.append(
                f"{i + 1}. {e['name']} (标准化名称: {e['normalizedName']}, 类型: {e['type']})"
            )

        prompt = f"""请查询以下实体（公司/组织）的注册地址信息。这些都是氢能领域的相关实体。

{chr(10).join(entities_info)}

## 要求

1. **必须进行联网查询**：使用搜索引擎查询每个实体的官方注册地址
2. **返回标准化地址**：省份、城市、区县（如有）
3. **准确性优先**：如果查不到准确信息，confidence设为0，不要猜测

## 输出格式

返回JSON数组:
```json
[
  {{
    "normalized_name": "实体标准化名称",
    "province": "XX省/市/自治区",
    "city": "XX市",
    "district": "XX区/县",
    "confidence": 0.95,
    "source": "查询来源说明"
  }}
]
```

只返回JSON,不要其他内容。"""

        return prompt

    async def process_batch_async(
            self,
            session: aiohttp.ClientSession,
            items: List[Dict],
            task_type: str
    ) -> Optional[List[Dict]]:
        """异步处理一批数据"""

        if task_type == 'green':
            prompt = self.build_green_classification_prompt(items)
        elif task_type == 'tech':
            prompt = self.build_tech_classification_prompt(items)
        elif task_type == 'location':
            prompt = self.build_location_extraction_prompt(items)
        else:
            return None

        response = await self.call_llm_async(session, prompt)
        if not response:
            return None

        try:
            # 提取JSON
            json_start = response.find('[')
            json_end = response.rfind(']') + 1
            if json_start >= 0 and json_end > json_start:
                json_str = response[json_start:json_end]
                results = json.loads(json_str)
                return results if isinstance(results, list) else []
        except json.JSONDecodeError as e:
            logger.warning(f"解析{task_type}结果失败: {e}")
            return None

    async def process_patents_async(self, session: aiohttp.ClientSession):
        """异步处理专利数据"""
        batch_size = LLM_ENHANCE_CONFIG['batch_size']

        while not self.should_stop:
            # 获取未处理的专利
            patents = self.get_unprocessed_patents(limit=batch_size * self.max_concurrent)
            if not patents:
                logger.info("✓ 所有专利已处理完成")
                break

            logger.info(f"获取到 {len(patents)} 个未处理专利")

            # 分批并发处理
            tasks = []
            for i in range(0, len(patents), batch_size):
                batch = patents[i:i + batch_size]
                patent_ids = [p['patentId'] for p in batch]

                # 绿色分类
                if LLM_ENHANCE_CONFIG['enable_green_classification']:
                    task = self.process_batch_async(session, batch, 'green')
                    tasks.append(('green', patent_ids, task))

                # 技术分类
                if LLM_ENHANCE_CONFIG['enable_tech_classification']:
                    task = self.process_batch_async(session, batch, 'tech')
                    tasks.append(('tech', patent_ids, task))

            # 并发执行
            results = await asyncio.gather(*[t[2] for t in tasks], return_exceptions=True)

            # 保存结果
            for i, (task_type, patent_ids, _) in enumerate(tasks):
                result = results[i]
                if result and not isinstance(result, Exception):
                    if task_type == 'green':
                        self.storage.append_data('green_classifications', result)
                        logger.info(f"  ✓ 已保存 {len(result)} 条绿色分类")
                    elif task_type == 'tech':
                        self.storage.append_data('tech_classifications', result)
                        logger.info(f"  ✓ 已保存 {len(result)} 条技术分类")

            # 标记为已处理
            all_patent_ids = [pid for _, pids, _ in tasks for pid in pids]
            self.storage.mark_processed_patents(list(set(all_patent_ids)))

            await asyncio.sleep(0.1)  # 避免过快

    async def process_entities_async(self, session: aiohttp.ClientSession):
        """异步处理实体数据"""
        if not LLM_ENHANCE_CONFIG['enable_location_extraction']:
            return

        batch_size = 10  # 实体批次小一些

        while not self.should_stop:
            entities = self.get_unprocessed_entities(limit=batch_size * 10)
            if not entities:
                logger.info("✓ 所有实体已处理完成")
                break

            logger.info(f"获取到 {len(entities)} 个未处理实体")

            # 分批处理
            for i in range(0, len(entities), batch_size):
                batch = entities[i:i + batch_size]
                entity_names = [e['normalizedName'] for e in batch]

                result = await self.process_batch_async(session, batch, 'location')
                if result:
                    self.storage.append_data('entity_locations', result)
                    logger.info(f"  ✓ 已保存 {len(result)} 条地理位置")

                self.storage.mark_processed_entities(entity_names)
                await asyncio.sleep(0.5)  # 地理位置查询慢一些

    async def generate_all_async(self):
        """异步生成所有数据（主入口）"""
        logger.info("=" * 60)
        logger.info("开始LLM数据生成（异步并发模式 - 从Excel读取数据）")
        logger.info(f"会话ID: {self.session_id}")
        logger.info(f"最大并发: {self.max_concurrent}")
        logger.info("=" * 60)

        start_time = datetime.now()

        # 初始化semaphore
        self.semaphore = asyncio.Semaphore(self.max_concurrent)

        # 统计总数
        total_patents = self.get_total_patents_count()
        total_entities = self.get_total_entities_count()
        processed = self.storage.get_processed_count()

        logger.info(
            f"专利总数: {total_patents}, 已处理: {processed['patents']}, 待处理: {total_patents - processed['patents']}")
        logger.info(
            f"实体总数: {total_entities}, 已处理: {processed['entities']}, 待处理: {total_entities - processed['entities']}")

        # 创建HTTP会话
        async with await self.create_session() as session:
            # 处理专利
            logger.info("\n" + "=" * 60)
            logger.info("处理专利数据")
            logger.info("=" * 60)
            await self.process_patents_async(session)

            # 处理实体
            logger.info("\n" + "=" * 60)
            logger.info("处理实体数据")
            logger.info("=" * 60)
            await self.process_entities_async(session)

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        # 打印统计
        stats = self.storage.get_stats()
        logger.info("\n" + "=" * 60)
        logger.info("数据生成完成")
        logger.info("=" * 60)
        logger.info(f"会话ID: {self.session_id}")
        logger.info(f"耗时: {duration:.2f} 秒")
        logger.info(f"\n生成统计:")
        logger.info(f"  - 已处理专利: {stats['processed_patents']}")
        logger.info(f"  - 已处理实体: {stats['processed_entities']}")
        logger.info(f"  - 绿色技术分类: {stats['green_classifications']} 条")
        logger.info(f"  - 技术领域分类: {stats['tech_classifications']} 条")
        logger.info(f"  - 实体地理位置: {stats['entity_locations']} 条")
        logger.info(f"\nLLM调用统计:")
        logger.info(f"  - 总请求数: {self.stats['total_requests']}")
        logger.info(f"  - 成功: {self.stats['successful_requests']}")
        logger.info(f"  - 失败: {self.stats['failed_requests']}")
        logger.info(f"  - 重试: {self.stats['retries']}")
        if self.stats['successful_requests'] > 0:
            avg_time = self.stats['total_time'] / self.stats['successful_requests']
            logger.info(f"  - 平均响应时间: {avg_time:.2f} 秒")
        logger.info("=" * 60)
        logger.info(f"\n下一步: 运行 llm_import_to_neo4j.py --session {self.session_id}")


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
    list_sessions_flag = False

    # 解析命令行参数
    if len(sys.argv) > 1:
        if sys.argv[1] == '--session':
            session_id = sys.argv[2] if len(sys.argv) > 2 else None
        elif sys.argv[1] == '--list':
            list_sessions_flag = True
        elif sys.argv[1] == '--new':
            session_id = None
        elif sys.argv[1] == '--help':
            print("用法:")
            print("  python llm_generate_json.py              # 自动恢复最新会话")
            print("  python llm_generate_json.py --new        # 强制创建新会话")
            print("  python llm_generate_json.py --session <session_id>  # 恢复指定会话")
            print("  python llm_generate_json.py --list       # 列出所有会话")
            return

    # 列表命令
    if list_sessions_flag:
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

    # 自动恢复最新会话
    if session_id is None and '--new' not in sys.argv:
        sessions = list_available_sessions()
        if sessions:
            session_id = sessions[0]['session_id']
            logger.info(f"⚙️  自动恢复最新会话: {session_id}")
            logger.info(f"    (使用 --new 参数可以强制创建新会话)")
        else:
            logger.info("⚙️  没有找到已有会话，将创建新会话")

    generator = AsyncLLMGenerator(session_id)

    try:
        # 运行异步主函数
        asyncio.run(generator.generate_all_async())
        logger.info("\n✓ 所有数据已成功生成并保存到JSON!")

    except Exception as e:
        logger.error(f"生成失败: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()