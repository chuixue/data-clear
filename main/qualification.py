#coding:utf8
'''
Created on Sep 9, 2016

@author: Administrator
'''
lspf = [
        "城乡规划编制",
        "工程招标代理",
        "房地产评估机构",
        "勘察劳务类工程勘察劳务资质",
        "岩土工程(分项)工程勘察专业资质",
        "水文地质工程勘察专业资质",
        "岩土工程工程勘察专业资质",
        "工程测量工程勘察专业资质",
        "工程勘察综合类工程勘察综合资质",
        "燃气燃烧器具安装、维修专项资质",
        "古建筑工程专业承包",
        "环保工程专业承包",
        "建筑机电安装工程专业承包",
        "特种工程专业承包",
        "民航空管工程及机场弱电系统工程专业承包",
        "电子与智能化工程专业承包",
        "铁路电气化工程专业承包",
        "金属门窗工程专业承包",
        "输变电工程专业承包",
        "水工金属结构制作与安装工程专业承包",
        "水利水电机电安装工程专业承包",
        "河湖整治工程专业承包",
        "模板脚手架专业承包",
        "核工程专业承包",
        "消防设施工程专业承包",
        "钢结构工程专业承包",
        "城市及道路照明工程专业承包",
        "公路交通工程专业承包公路机电工程",
        "铁路铺轨架梁工程专业承包",
        "机场场道工程专业承包",
        "预拌混凝土专业承包",
        "防水防腐保温工程专业承包",
        "通航建筑物工程专业承包",
        "公路交通工程专业承包公路安全设施",
        "港航设备安装及水上交管工程专业承包",
        "土石方工程专业承包",
        "地基基础专业承包",
        "桥梁工程专业承包",
        "高耸构筑物工程专业承包",
        "机场目视助航工程专业承包",
        "建筑幕墙工程专业承包",
        "建筑装修装饰工程专业承包",
        "公路路基工程专业承包",
        "地基基础工程专业承包",
        "起重设备安装工程专业承包",
        "铁路电务工程专业承包",
        "公路路面工程专业承包",
        "混凝土预制构件专业承包",
        "隧道工程专业承包",
        "建筑装饰装修工程设计与施工一体化",
        "消防设施工程设计与施工一体化",
        "建筑智能化工程设计与施工一体化",
        "建筑幕墙工程设计与施工一体化",
        "模板作业劳务分包",
        "混凝土作业劳务分包",
        "钢筋作业劳务分包",
        "脚手架作业劳务分包",
        "石制作业劳务分包",
        "钣金作业劳务分包",
        "焊接作业劳务分包",
        "木工作业劳务分包",
        "抹灰作业劳务分包",
        "水暖电安装作业劳务分包",
        "油漆作业劳务分包",
        "架线作业劳务分包",
        "砌筑作业劳务分包",
        "建筑工程施工总承包",
        "机电安装工程施工总承包",
        "冶金工程施工总承包",
        "水利水电工程施工总承包",
        "铁路工程施工总承包",
        "石油化工工程施工总承包",
        "市政公用工程施工总承包",
        "机电工程施工总承包",
        "通信工程施工总承包",
        "电力工程施工总承包",
        "港口与航道工程施工总承包",
        "房屋建筑工程施工总承包",
        "矿山工程施工总承包",
        "公路工程施工总承包",
        "建筑智能化系统设计专项资质",
        "建筑装饰工程设计专项资质",
        "环境工程设计专项资质",
        "建筑智能化工程设计专项资质",
        "建筑幕墙工程设计专项资质",
        "消防设施工程设计专项资质",
        "风景园林工程设计专项资质",
        "轻型钢结构工程设计专项资质",
        "建筑智能化系统工程设计专项资质",
        "环境工程工程设计专项资质",
        "照明工程设计专项资质",
        "建筑设计事务所建筑工程设计事务所资质",
        "结构设计事务所建筑工程设计事务所资质",
        "机电设计事务所建筑工程设计事务所资质",
        "冶金行业工程设计专业资质",
        "建筑行业工程设计行业资质",
        "机械行业工程设计行业资质",
        "水利行业工程设计行业资质",
        "市政（燃气工程、轨道交通工程除外）工程设计行业资质",
        "化工石化医药行业工程设计专业资质",
        "核工业行业工程设计专业资质",
        "建筑行业（建筑工程）工程设计专业资质",
        "轻纺行业（纺织工程）工程设计行业资质",
        "机械行业工程设计专业资质",
        "商物粮行业工程设计行业资质",
        "水利行业工程设计专业资质",
        "石油天然气（海洋石油）行业工程设计专业资质",
        "商物粮行业工程设计专业资质",
        "公路行业工程设计行业资质",
        "军工行业工程设计专业资质",
        "电力工程设计行业资质",
        "建材行业工程设计专业资质",
        "电力行业工程设计专业资质",
        "农林行业(农业工程)工程设计专业资质",
        "轻纺行业工程设计专业资质",
        "石油天然气（海洋石油）行业工程设计行业资质",
        "农林行业工程设计专业资质",
        "冶金行业工程设计行业资质",
        "电子通信广电行业工程设计专业资质",
        "市政行业工程设计专业资质",
        "建筑行业工程设计专业资质",
        "市政行业工程设计行业资质",
        "公路行业工程设计专业资质",
        "化工石化医药行业（化工工程）工程设计专业资质",
        "水运行业工程设计行业资质",
        "铁道行业工程设计行业资质",
        "市政（燃气工程、轨道交通工程除外）工程设计专业资质",
        "建材行业工程设计行业资质",
        "化工石化医药行业工程设计行业资质",
        "煤炭行业工程设计专业资质",
        "化工石油工程监理",
        "电力工程监理",
        "公路工程监理",
        "机电安装工程监理",
        "通信工程监理",
        "矿山工程监理",
        "农林工程监理",
        "房屋建筑工程监理",
        "冶炼工程监理",
        "铁路工程监理",
        "市政公用工程监理",
        "水利水电工程监理",
        "航天航空工程监理",
        "工程监理综合资质"
]