# -*- coding: utf-8 -*-
"""
技术领域树定义（完整版）
三级层次结构: L1(产业链) -> L2(技术路径) -> L3(细分技术)
包含所有"其他"分类节点
"""

TECH_TREE = {
    "H1": {
        "nameZh": "制氢(上游)",
        "nameEn": "Hydrogen Production",
        "level": 1,
        "children": {
            "H1.1": {
                "nameZh": "电解水制氢",
                "nameEn": "Water Electrolysis",
                "level": 2,
                "children": {
                    "H1.1.1": {"nameZh": "碱性电解(AEL)", "nameEn": "Alkaline Electrolysis (AEL)", "level": 3},
                    "H1.1.2": {"nameZh": "质子交换膜电解(PEMEL)",
                               "nameEn": "Proton Exchange Membrane Electrolysis (PEMEL)", "level": 3},
                    "H1.1.3": {"nameZh": "固体氧化物电解(SOEC)", "nameEn": "Solid Oxide Electrolysis Cell (SOEC)",
                               "level": 3},
                    "H1.1.4": {"nameZh": "阴离子交换膜电解(AEMEL)",
                               "nameEn": "Anion Exchange Membrane Electrolysis (AEMEL)", "level": 3},
                    "H1.1.5": {"nameZh": "其他电解水制氢技术", "nameEn": "Other Water Electrolysis", "level": 3}
                }
            },
            "H1.2": {
                "nameZh": "化石能源制氢",
                "nameEn": "Fossil-based Reforming",
                "level": 2,
                "children": {
                    "H1.2.1": {"nameZh": "天然气重整(SMR)", "nameEn": "Steam Methane Reforming (SMR)", "level": 3},
                    "H1.2.2": {"nameZh": "煤制氢(煤气化)", "nameEn": "Coal Gasification", "level": 3},
                    "H1.2.3": {"nameZh": "甲醇裂解制氢", "nameEn": "Methanol Reforming", "level": 3},
                    "H1.2.4": {"nameZh": "其他化石能源制氢技术", "nameEn": "Other Fossil-based Reforming", "level": 3}
                }
            },
            "H1.3": {
                "nameZh": "可再生能源耦合制氢",
                "nameEn": "Renewable-integrated Production",
                "level": 2,
                "children": {
                    "H1.3.1": {"nameZh": "风光电制氢系统集成", "nameEn": "Wind-Solar-H2 System Integration",
                               "level": 3},
                    "H1.3.2": {"nameZh": "离网/微网制氢", "nameEn": "Off-grid/Microgrid H2 Production", "level": 3},
                    "H1.3.3": {"nameZh": "电解槽与可再生能源协同控制",
                               "nameEn": "Electrolyzer-Renewable Energy Co-control", "level": 3},
                    "H1.3.4": {"nameZh": "其他可再生能源耦合制氢技术",
                               "nameEn": "Other Renewable-integrated Production", "level": 3}
                }
            },
            "H1.4": {
                "nameZh": "光/热/生物制氢",
                "nameEn": "Alternative (Photo/Thermo/Bio) Methods",
                "level": 2,
                "children": {
                    "H1.4.1": {"nameZh": "光催化/光电化学制氢", "nameEn": "Photo-catalytic/Photoelectrochemical H2",
                               "level": 3},
                    "H1.4.2": {"nameZh": "太阳能热化学循环制氢", "nameEn": "Solar Thermochemical Cycles", "level": 3},
                    "H1.4.3": {"nameZh": "生物质气化/发酵制氢", "nameEn": "Biomass Gasification/Fermentation",
                               "level": 3},
                    "H1.4.4": {"nameZh": "其他光/热/生物制氢技术", "nameEn": "Other Alternative Methods", "level": 3}
                }
            },
            "H1.5": {
                "nameZh": "其他制氢技术",
                "nameEn": "Other Hydrogen Production",
                "level": 2,
                "children": {}
            }
        }
    },
    "H2": {
        "nameZh": "储运氢(中游)",
        "nameEn": "Hydrogen Storage & Transportation",
        "level": 1,
        "children": {
            "H2.1": {
                "nameZh": "高压气态储运",
                "nameEn": "High-pressure Gaseous",
                "level": 2,
                "children": {
                    "H2.1.1": {"nameZh": "35 MPa 储氢容器", "nameEn": "35 MPa H2 Containers", "level": 3},
                    "H2.1.2": {"nameZh": "70 MPa 储氢容器", "nameEn": "70 MPa H2 Containers", "level": 3},
                    "H2.1.3": {"nameZh": "高压管束车/长管拖车", "nameEn": "High-pressure Tube Trailers", "level": 3},
                    "H2.1.4": {"nameZh": "其他高压气态储运技术", "nameEn": "Other High-pressure Gaseous", "level": 3}
                }
            },
            "H2.2": {
                "nameZh": "低温液态储运",
                "nameEn": "Cryogenic Liquid",
                "level": 2,
                "children": {
                    "H2.2.1": {"nameZh": "液氢储罐(车载/固定)", "nameEn": "Liquid H2 Tanks (Mobile/Stationary)",
                               "level": 3},
                    "H2.2.2": {"nameZh": "液氢加注与转注技术", "nameEn": "Liquid H2 Transfer Technology", "level": 3},
                    "H2.2.3": {"nameZh": "液氢蒸发损失控制", "nameEn": "Liquid H2 Boil-off Control", "level": 3},
                    "H2.2.4": {"nameZh": "其他低温液态储运技术", "nameEn": "Other Cryogenic Liquid", "level": 3}
                }
            },
            "H2.3": {
                "nameZh": "固态/材料储氢",
                "nameEn": "Solid-state / Material-based",
                "level": 2,
                "children": {
                    "H2.3.1": {"nameZh": "金属/合金氢化物", "nameEn": "Metal/Alloy Hydrides", "level": 3},
                    "H2.3.2": {"nameZh": "化学氢化物(NaBH₄, NH₃BH₃ 等)", "nameEn": "Chemical Hydrides", "level": 3},
                    "H2.3.3": {"nameZh": "多孔材料吸附储氢(MOFs, 碳材料)", "nameEn": "Porous Materials (MOFs, Carbon)",
                               "level": 3},
                    "H2.3.4": {"nameZh": "其他固态/材料储氢技术", "nameEn": "Other Solid-state Storage", "level": 3}
                }
            },
            "H2.4": {
                "nameZh": "有机载体储运(LOHC等)",
                "nameEn": "Liquid Organic Carriers",
                "level": 2,
                "children": {
                    "H2.4.1": {"nameZh": "甲苯/甲基环己烷体系", "nameEn": "Toluene/Methylcyclohexane System",
                               "level": 3},
                    "H2.4.2": {"nameZh": "N-乙基咔唑等杂环体系", "nameEn": "N-Ethylcarbazole Systems", "level": 3},
                    "H2.4.3": {"nameZh": "载体加氢/脱氢催化剂", "nameEn": "Hydrogenation/Dehydrogenation Catalysts",
                               "level": 3},
                    "H2.4.4": {"nameZh": "其他有机载体储运技术", "nameEn": "Other Liquid Organic Carriers", "level": 3}
                }
            },
            "H2.5": {
                "nameZh": "氢输配基础设施",
                "nameEn": "Distribution Infrastructure",
                "level": 2,
                "children": {
                    "H2.5.1": {"nameZh": "氢气管道(纯氢/掺氢)", "nameEn": "H2 Pipelines (Pure/Blended)", "level": 3},
                    "H2.5.2": {"nameZh": "加氢站关键设备", "nameEn": "H2 Refueling Station Equipment", "level": 3},
                    "H2.5.3": {"nameZh": "氢气压缩与纯化", "nameEn": "H2 Compression & Purification", "level": 3},
                    "H2.5.4": {"nameZh": "其他氢输配基础设施技术", "nameEn": "Other Distribution Infrastructure",
                               "level": 3}
                }
            },
            "H2.6": {
                "nameZh": "其他储运氢技术",
                "nameEn": "Other Storage & Transportation",
                "level": 2,
                "children": {}
            }
        }
    },
    "H3": {
        "nameZh": "用氢(下游)",
        "nameEn": "Hydrogen Utilization",
        "level": 1,
        "children": {
            "H3.1": {
                "nameZh": "燃料电池",
                "nameEn": "Fuel Cells",
                "level": 2,
                "children": {
                    "H3.1.1": {"nameZh": "质子交换膜燃料电池(PEMFC)",
                               "nameEn": "Proton Exchange Membrane Fuel Cell (PEMFC)", "level": 3},
                    "H3.1.2": {"nameZh": "固体氧化物燃料电池(SOFC)", "nameEn": "Solid Oxide Fuel Cell (SOFC)",
                               "level": 3},
                    "H3.1.3": {"nameZh": "碱性燃料电池(AFC)等其他类型", "nameEn": "Alkaline Fuel Cell (AFC) & Others",
                               "level": 3},
                    "H3.1.4": {"nameZh": "其他燃料电池技术", "nameEn": "Other Fuel Cells", "level": 3}
                }
            },
            "H3.2": {
                "nameZh": "氢能交通",
                "nameEn": "Hydrogen Mobility",
                "level": 2,
                "children": {
                    "H3.2.1": {"nameZh": "氢燃料电池汽车", "nameEn": "Fuel Cell Electric Vehicles", "level": 3},
                    "H3.2.2": {"nameZh": "氢动力轨道交通/船舶/航空", "nameEn": "H2 Rail/Marine/Aviation", "level": 3},
                    "H3.2.3": {"nameZh": "车载供氢系统", "nameEn": "On-board H2 Supply Systems", "level": 3},
                    "H3.2.4": {"nameZh": "其他氢能交通技术", "nameEn": "Other Hydrogen Mobility", "level": 3}
                }
            },
            "H3.3": {
                "nameZh": "工业用氢",
                "nameEn": "Industrial Applications",
                "level": 2,
                "children": {
                    "H3.3.1": {"nameZh": "氢冶金(直接还原铁等)", "nameEn": "H2 Metallurgy (DRI, etc.)", "level": 3},
                    "H3.3.2": {"nameZh": "石化/合成氨/甲醇用氢", "nameEn": "Petrochemical/Ammonia/Methanol",
                               "level": 3},
                    "H3.3.3": {"nameZh": "电子级高纯氢应用", "nameEn": "Electronics-grade H2", "level": 3},
                    "H3.3.4": {"nameZh": "其他工业用氢技术", "nameEn": "Other Industrial Applications", "level": 3}
                }
            },
            "H3.4": {
                "nameZh": "氢能发电与储能",
                "nameEn": "Power Generation & Grid Storage",
                "level": 2,
                "children": {
                    "H3.4.1": {"nameZh": "氢燃气轮机发电", "nameEn": "H2 Gas Turbine Power", "level": 3},
                    "H3.4.2": {"nameZh": "氢-电双向储能系统", "nameEn": "H2-Electricity Bidirectional Storage",
                               "level": 3},
                    "H3.4.3": {"nameZh": "氢参与电网调峰", "nameEn": "H2 Grid Balancing", "level": 3},
                    "H3.4.4": {"nameZh": "其他氢能发电与储能技术", "nameEn": "Other Power Generation & Storage",
                               "level": 3}
                }
            },
            "H3.5": {
                "nameZh": "氢基燃料合成",
                "nameEn": "Hydrogen-derived Fuels",
                "level": 2,
                "children": {
                    "H3.5.1": {"nameZh": "绿氨合成", "nameEn": "Green Ammonia Synthesis", "level": 3},
                    "H3.5.2": {"nameZh": "电子甲醇/e-Fuels", "nameEn": "e-Methanol/e-Fuels", "level": 3},
                    "H3.5.3": {"nameZh": "合成航空煤油(e-Kerosene)", "nameEn": "e-Kerosene", "level": 3},
                    "H3.5.4": {"nameZh": "其他氢基燃料合成技术", "nameEn": "Other Hydrogen-derived Fuels", "level": 3}
                }
            },
            "H3.6": {
                "nameZh": "其他用氢技术",
                "nameEn": "Other Hydrogen Utilization",
                "level": 2,
                "children": {}
            }
        }
    },
    "H4": {
        "nameZh": "其他(不在制、储运、用之中)",
        "nameEn": "Other (Not in Production/Storage/Utilization)",
        "level": 1,
        "children": {}
    }
}


def get_all_tech_domains():
    """
    展开技术领域树，返回所有节点的列表
    每个节点包含: code, nameZh, nameEn, level, parent_code
    """
    domains = []

    def traverse(node_dict, parent_code=None):
        for code, data in node_dict.items():
            domain = {
                "code": code,
                "nameZh": data["nameZh"],
                "nameEn": data["nameEn"],
                "level": data["level"],
                "parent_code": parent_code
            }
            domains.append(domain)

            if "children" in data and data["children"]:
                traverse(data["children"], parent_code=code)

    traverse(TECH_TREE)
    return domains


def get_tech_tree_text():
    """
    生成技术树的文本描述（用于LLM提示）
    """
    text_lines = []

    text_lines.append("## 氢能技术领域分类体系")
    text_lines.append("")

    for l1_code, l1_data in TECH_TREE.items():
        text_lines.append(f"### {l1_code} {l1_data['nameZh']} ({l1_data['nameEn']})")

        if not l1_data.get('children'):
            text_lines.append("")
            continue

        for l2_code, l2_data in l1_data['children'].items():
            text_lines.append(f"  **{l2_code}** {l2_data['nameZh']} ({l2_data['nameEn']})")

            if l2_data.get('children'):
                for l3_code, l3_data in l2_data['children'].items():
                    text_lines.append(f"    - {l3_code}: {l3_data['nameZh']}")

            text_lines.append("")

    return "\n".join(text_lines)


if __name__ == "__main__":
    # 测试
    domains = get_all_tech_domains()
    print(f"共有 {len(domains)} 个技术领域节点")
    for d in domains[:20]:
        print(d)

    print("\n" + "=" * 60)
    print(get_tech_tree_text()[:500])




