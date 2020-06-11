#!/usr/bin/env python
# coding=utf-8
# author: zengyuetian
# 此代码仅供学习与交流，请勿用于商业用途。
# 爬取小区数据的爬虫派生类


import re
import threadpool
import math
from bs4 import BeautifulSoup
from lib.item.xiaoqu import *
from lib.item.loupan import *
from lib.zone.city import get_city
from lib.spider.base_spider import *
from lib.utility.date import *
from lib.utility.path import *
from lib.zone.area import *
from lib.utility.log import *
import lib.utility.version
from lib.zone.city import *


class XqLpSpider(BaseSpider):
    def __init__(self, name):
        super().__init__(name)
        self.today_path = "."

    def get_xiaoqu_loupan_info(self, city_name, page_index, is_xiaoqu):
        if is_xiaoqu:
            info = self.get_xiaoqu_info(city_name, page_index)
        else:
            info = self.get_loupan_info(city_name, page_index)
        return info

    def collect_city_data(self, city_name, is_xiaoqu, fmt='csv'):
        """
        对于每个板块,获得这个板块下所有小区的信息
        :param is_xiaoqu:
        :param city_name:
        :param fmt:
        :return:
        """
        self.today_path = create_date_path("{0}/xqlp".format(SPIDER_NAME), city_name, self.date_string)
        csv_file_name = "/{0}.csv".format(city_name)
        csv_file = self.today_path + csv_file_name
        # 获取总页数
        page_count = self.get_city_page_count(city_name)
        if page_count == 0:
            print(f"{city_name} page NULL, skip visit...")
            return
        f = open(csv_file, 'w')
        try:
            for i in range(page_count):
                page_index = i + 1
                print(f"Visit {city_name} pg{page_index}")
                # 开始获得需要的板块数据
                xqs = self.get_xiaoqu_loupan_info(city_name, page_index, is_xiaoqu)
                # 锁定
                if self.mutex.acquire(1):
                    self.total_num += len(xqs)
                    # 释放
                    self.mutex.release()
                if fmt == "csv":
                    for xiaoqu in xqs:
                        f.write(self.date_string + "," + xiaoqu.text() + "\n")
                print(f"Finish Save {city_name} Save to {csv_file}")
                logger.info(f"Finish Save {city_name} Save to {csv_file}")
        except Exception as e:
            print(f"{str(e)}")
        finally:
            f.close()

    @staticmethod
    def get_xiaoqu_info(city_name, page_index):
        total_page = 1
        xiaoqu_list = list()
        page = 'http://{0}.{1}.com/xiaoqu/pg{2}'.format(city_name, SPIDER_NAME, page_index)
        print(f"{city_name}-{page}")
        logger.info(page)
        # 直接把所有小区爬下来
        headers = create_headers()
        BaseSpider.random_delay()
        response = requests.get(page, timeout=10, headers=headers)
        html = response.content
        soup = BeautifulSoup(html, "lxml")
        # 获得有小区信息的panel
        house_elems = soup.find_all('li', attrs={"class":["xiaoquListItem", "xiaoquListItemRight"]})
        for house_elem in house_elems:
            price = house_elem.find('div', class_="totalPrice")
            name = house_elem.find('div', class_='title')
            on_sale = house_elem.find('div', class_="xiaoquListItemSellCount")
            # 继续清理数据
            price = price.text.strip()
            name = name.text.replace("\n", "")
            on_sale = on_sale.text.replace("\n", "").strip()
            # 作为对象保存
            xiaoqu = XiaoQu("", "", name, price, on_sale)
            xiaoqu_list.append(xiaoqu)
        return xiaoqu_list

    @staticmethod
    def get_loupan_info(city_name, page_index):
        """
        爬取页面获取城市新房楼盘信息
        :param page_index: 页码索引
        :param city_name: 城市
        :return: 新房楼盘信息列表
        """
        loupan_list = list()
        page = 'http://{0}.{1}.com/loupan/pg{0}'.format(city_name, SPIDER_NAME, page_index)
        print(f"{city_name}-{page}")
        headers = create_headers()
        BaseSpider.random_delay()
        response = requests.get(page, timeout=10, headers=headers)
        html = response.content
        soup = BeautifulSoup(html, "lxml")
        # 获得有小区信息的panel
        house_elements = soup.find_all('li', class_="resblock-list")
        for house_elem in house_elements:
            price = house_elem.find('span', class_="number")
            total = house_elem.find('div', class_="second")
            loupan = house_elem.find('a', class_='name')
            # 继续清理数据
            try:
                price = price.text.strip()
            except Exception as e:
                price = '0'
            loupan = loupan.text.replace("\n", "")
            try:
                total = total.text.strip().replace(u'总价', '')
                total = total.replace(u'/套起', '')
            except Exception as e:
                total = '0'
            print("{0} {1} {2} ".format(
                loupan, price, total))
            # 作为对象保存
            loupan = LouPan(loupan, price, total)
            loupan_list.append(loupan)
        return loupan_list

    def get_province_info(self):
        """
        获取全国的省份-城市的关系数据
        :return:
        """
        main_url = beike_main_page
        print(f"BeiKe's main page is {main_url}")
        if main_url in (None, ""):
            return None
        # 访问主页，并且解析主页所有省份+行政区
        headers = create_headers()
        response = requests.get(main_url, timeout=10, headers=headers)
        html = response.content
        soup = BeautifulSoup(html, "lxml")
        # 遍历所有元素
        province_lst = soup.find_all('li', class_="city_list_li city_list_li_selected")
        # 数据存储map【prov_name, list[city_url...]】
        province_map = dict()
        # 存储数据到list
        city_name_list = list()
        # 如果当前地区没有房产信息，那么就会出现len为空的现象
        if len(province_lst) == 0:
            print(f"China province list is empty")
            return None
        for province in province_lst:
            prov = province.find(class_="city_list_tit c_b")
            prov_name = prov.contents[0].replace(' ', '').replace('\t', '').replace('\n', '')
            # 过滤出国内省份
            a_lst = prov.find_all('a')
            if len(a_lst) != 0:
                print(f"Occur foreign city: {str(prov_name)}")
                continue
            print(f"prov[{prov_name}]")
            # 处理国内省份
            cites = province.find_all('a')
            if len(cites) == 0:
                print(f"{prov_name} has no cities list, continue handle other province")
                continue
            cites_url_list = list()
            for city in cites:
                city_chinese_name = city.contents[0].replace(' ', '').replace('\t', '').replace('\n', '')
                city_href = city["href"]
                city_name = city_href.split('.')[0].replace('/', '')
                is_xiaoqu = True if city_href.split('.')[1] != 'fang' else False
                city_url = f"http:{city_href}"
                print(f"{prov_name}-{city_chinese_name}- {city_name}-{city_url}")
                # 保存地址
                cites_url_list.append(city_url)
                city_url_front = f"{city_name}" if is_xiaoqu else f"{city_name}.fang"
                city_name_list.append(city_url_front)
            # 保存进入省信息
            province_map[str(prov_name)] = cites_url_list
        # 返回结果
        return city_name_list

    def get_city_page_count(self, city_name):
        """
        获取城市的页面数
        :param city_name: xx.fang or xx
        :return:
        """
        headers = create_headers()
        if city_name.endswith('.fang'):
            city_url = 'http://{0}.{1}.com/loupan/'.format(city_name, SPIDER_NAME)
            response = requests.get(city_url, timeout=10, headers=headers)
            html = response.content
            soup = BeautifulSoup(html, "lxml")
            # 获得总的页数
            try:
                page_box = soup.find_all('div', class_='page-box')[0]
                matches = re.search('.*data-total-count="(\d+)".*', str(page_box))
                page_count = int(math.ceil(int(matches.group(1)) / 10))
            except Exception as e:
                print(f"Exception: {city_name} get total page count, {str(e)}, {city_url}")
                page_count = 0
        else:
            city_url = 'http://{0}.{1}.com/xiaoqu/'.format(city_name, SPIDER_NAME)
            response = requests.get(city_url, timeout=10, headers=headers)
            html = response.content
            soup = BeautifulSoup(html, "lxml")
            # 遍历所有元素
            try:
                page_box = soup.find_all('div', attrs=["page_box", "house-lst-page-box"])[0]
                page_count = int(page_box["page-data"].split(',')[0].split(':')[1])
            except Exception as e:
                print(f"Exception: {city_name} get total page count, {str(e)}, {city_url}")
                page_count = 0
        # 返回页码
        return page_count

    def start(self):
        """
        自动获取所有的行政区小区数据
        :return:
        """
        t1 = time.time()
        # 获取省份-城市信息
        city_info = self.get_province_info()
        print(f"China has {len(city_info)} city")
        # 准备线程池用到的参数((city_name, page_index, is_xiaoqu), (...), ...)
        pool_size = thread_pool_size
        pool = threadpool.ThreadPool(pool_size)
        is_xiaoqu_list = [(False if city.endswith(".fang") else True) for city in city_info]
        nones = [None for i in range(len(city_info))]
        args = zip(zip(city_info, is_xiaoqu_list), nones)
        print(args)
        my_requests = threadpool.makeRequests(callable_=self.collect_city_data, args_list=args)
        [pool.putRequest(req) for req in my_requests]
        pool.wait()
        pool.dismissWorkers(pool_size, do_join=True)  # 完成后退出
        # 计算总计花了多久
        t2 = time.time()
        print("Total cost {0} second to crawl {1} data items.".format(t2 - t1, self.total_num))
