# -*- coding: utf-8 -*-
"""
配置文件
"""
# Neo4j数据库配置
NEO4J_CONFIG = {
    'uri': 'bolt://8.130.70.72:21099',
    'user': 'neo4j',
    'password': 'zyz_password'
}

# LLM配置(OpenAI兼容API)
LLM_CONFIG = {
    'api_base': 'https://ai.gitee.com/v1',
    'api_key': 'BRGYKADH3CKIAGSETETJBD95C1TAKPICNJMY22II',
    'model': 'Qwen2.5-72B-Instruct',
    'temperature': 0.1,
    'max_tokens': 2000,
    'timeout': 30  # 单个请求超时时间
}

# 数据文件配置
DATA_CONFIG = {
    'data_dir': './data/',
    # 文件名到技术领域代码的映射(初步分类，LLM会进一步细化)
    'file_mapping': {
        "2.6.1制氢技术-1.xlsx": "H1.1",
        "2.6.1制氢技术-2.xlsx": "H1.1",
        "2.6.2.1 物理储氢.xlsx": "H2.1",
        "2.6.2.2合金储氢.xlsx": "H2.3",
        "2.6.2.3无机储氢-1.xlsx": "H2.3",
        "2.6.2.3无机储氢-2.xlsx": "H2.3",
        "2.6.2.4有机储氢.xlsx": "H2.4",
        "2.6.3氢燃料电池.xlsx": "H3.1",
        "2.6.4氢制冷.xlsx": "H3.4"
    }
}

# 日志配置
LOG_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s - %(levelname)s - %(message)s',
    'file': 'import_log.txt'
}

# 绿色技术分类定义
GREEN_TECH_CATEGORIES = {
    '零碳使能型': {
        'code': 'GT1',
        'categoryType': '零碳使能型',
        'definition': '直接支撑绿氢经济的核心技术，必须或显著提升可再生能源制氢的可行性，降低绿氢成本、能耗或间歇性障碍，实现全流程近零碳排放。',
        'typicalExamples': ['PEM电解槽优化', '风光氢离网耦合控制系统', '低能耗有机液体储氢（LOHC）脱氢技术', '绿氢燃料电池重卡']
    },
    '低碳过渡型': {
        'code': 'GT2',
        'categoryType': '低碳过渡型',
        'definition': '支持蓝氢或显著减碳的过渡性技术，依赖碳捕集与封存（CCS/CCUS）实现大幅减排（建议捕集率≥85%），或用于替代高碳终端应用（如煤制氨转为氢制氨）。',
        'typicalExamples': ['蒸汽甲烷重整（SMR）+高效CCS系统', '蓝氢专用输氢管道', '氢基直接还原铁（H₂-DRI）炼钢工艺']
    },
    '绿氢兼容型': {
        'code': 'GT3',
        'categoryType': '绿氢兼容型',
        'definition': '技术本身不产生碳排放，可用于绿氢也可用于灰氢，无化石能源依赖，不锁定高碳路径，但也不专属促进零碳转型。',
        'typicalExamples': ['高压IV型储氢瓶', '标准加氢站设备', '通用质子交换膜（PEM）', '氢气压缩机']
    },
    '碳锁定型': {
        'code': 'GT4',
        'categoryType': '碳锁定型',
        'definition': '强化高碳制氢路径，仅优化灰氢或棕氢工艺，延长化石能源制氢寿命，且未集成CCS或可再生电力接口,阻碍系统脱碳。',
        'typicalExamples': ['无CCS的SMR催化剂效率提升', '炼油厂专用灰氢提纯装置', '基于天然气管网的高比例灰氢输配系统']
    },
    '中性模糊型': {
        'code': 'GT5',
        'categoryType': '中性模糊型',
        'definition': '无法明确判断其绿色贡献的技术，通常为通用基础部件、安全装置或未限定应用场景/能源来源的模块。',
        'typicalExamples': ['氢气泄漏传感器', '密封材料改进', '未限定电源类型的电解电源管理模块', '通用阀门或连接件']
    }
}

# LLM增强配置 - 优化后的并发配置
# LLM增强配置 - 修复超时问题
LLM_ENHANCE_CONFIG = {
    'batch_size': 10,  # 降低批次大小
    'output_dir': './llm_output_previous/',
    'enable_tech_classification': True,
    'enable_green_classification': True,
    'enable_location_extraction': True,
    'max_concurrent_requests': 5,  # 降低并发！从30→5
    'max_retries': 3,
    'retry_delay': 3,  # 增加重试延迟
    'request_timeout': 120,  # 增加超时时间
    'connection_limit': 50,  # 降低连接池
    'rate_limit_per_minute': 100  # 降低速率限制
}