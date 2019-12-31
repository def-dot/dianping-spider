# -*- coding: utf-8 -*-
from urllib.parse import quote
import string
import re
import requests
from fontTools.ttLib import TTFont
from bs4 import BeautifulSoup
import bs4
from fake_useragent import UserAgent
import psycopg2
import sys

from common import parse_char

# number
numberfont = TTFont('font_woff/number.woff')

# address
addressfont = TTFont('font_woff/address.woff')

# shopdesc
shopdescfont = TTFont('font_woff/shopdesc.woff')

# hours
hoursfont = TTFont('font_woff/hours.woff')

number_TTGlyphs = numberfont['cmap'].tables[0].ttFont.getGlyphOrder()[2:]
address_TTGlyphs = addressfont['cmap'].tables[0].ttFont.getGlyphOrder()[2:]
shopdesc_TTGlyphs = shopdescfont['cmap'].tables[0].ttFont.getGlyphOrder()[2:]
hours_TTGlyphs = hoursfont['cmap'].tables[0].ttFont.getGlyphOrder()[2:]

number_dict = {}
for i, x in enumerate(number_TTGlyphs):
    number_dict[x] = i
address_dict = {}
for i, x in enumerate(address_TTGlyphs):
    address_dict[x] = i
shopdesc_dict = {}
for i, x in enumerate(shopdesc_TTGlyphs):
    shopdesc_dict[x] = i
hours_dict = {}
for i, x in enumerate(hours_TTGlyphs):
    hours_dict[x] = i

def parse_content(contents):
    result = list()
    for content in contents:
        if isinstance(content, bs4.element.NavigableString):
            result.append(str(content))
        elif isinstance(content, bs4.element.Tag):
            attr_class = content.attrs.get("class")
            text = content.text
            if not attr_class:
                result.append(text)
            elif attr_class[0] == "address":
                parsed_char = parse_char(text, address_TTGlyphs, address_dict)
                result.append(parsed_char)
            elif attr_class[0] == "num":
                parsed_char = parse_char(text, number_TTGlyphs, number_dict)
                result.append(parsed_char)
            elif attr_class[0] == "shopdesc":
                parsed_char = parse_char(text, shopdesc_TTGlyphs, shopdesc_dict)
                result.append(parsed_char)
            elif attr_class[0] == "hours":
                parsed_char = parse_char(text, hours_TTGlyphs, hours_dict)
                result.append(parsed_char)
            else:
                result.append(text)
    return ''.join(result)

def get_shop_name(soup):
    shopname = soup.find('h1', {"class": "shop-name"})
    return shopname.next

def get_address(soup):
    contents = soup.find_all('span', {"id": "address"})[0].contents
    result = parse_content(contents)
    return result

def get_location(address):
    lng, lat, district = None, None, None
    try:
        address = address.replace("#", "")
        url = r"https://apis.map.qq.com/ws/geocoder/v1/?address=天津市%s&key=DXEBZ-35MWX-3ZE4D-76E2C-C5DUH-XHBXS" % address
        url = quote(url, safe=string.printable)
        response = requests.get(url)
        response = response.json()
        lng = response["result"]["location"]["lng"]
        lat = response["result"]["location"]["lat"]
        district = response["result"]["address_components"]["district"]
    except Exception as e:
        print(str(e))
    return lng, lat, district

def get_telphonenumber(soup):
    contents = soup.find_all('p', {"class": "expand-info tel"})[0].contents
    result = parse_content(contents)
    return result

def get_avgprice(soup):
    contents = soup.find_all('span', {"id": "avgPriceTitle"})[0].contents
    result = parse_content(contents)
    return result

def get_open_time(soup):
    contents = soup.find_all('p', {"class": "info info-indent"})[0].contents[3].contents
    result = parse_content(contents)
    return result

headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'zh-CN,zh;q=0.9,zh-TW;q=0.8,en-US;q=0.7,en;q=0.6',
    'Cache-Control': 'max-age=0',
    'Host': 'www.dianping.com',
    'Referer': 'http://www.dianping.com/tianjin/',
    'Upgrade-Insecure-Requests': '1',
}

def get_proxy():
    return requests.get("http://120.26.217.149:5010/get/").text

def delete_proxy(proxy):
    requests.get("http://120.26.217.149:5010/delete/?proxy={}".format(proxy))

def get_html(url):
    for i in range(10):
        retry_count = 5
        proxy = get_proxy()
        print(proxy)
        while retry_count > 0:
            try:
                headers["User-Agent"] = UserAgent().random
                html = requests.get(url, headers=headers, timeout=15, proxies={"http": "http://{}".format(proxy)})
                resource = html.text
                if 'o-favor' in resource or 'shop-name' in resource:
                    return resource
                retry_count -= 1
            except Exception as e:
                print(str(e))
                retry_count -= 1
        # 出错5次, 删除代理池中代理
        delete_proxy(proxy)
    return None

db = psycopg2.connect(database="draw", user="postgres", password="", host="", port="5432")
cursor = db.cursor()
try:
    sql = "SELECT shop_id FROM dianping_info"
    cursor.execute(sql)
    data = cursor.fetchall()
    db_shop_ids = list()
    for item in data:
        shop_id = item[0]
        db_shop_ids.append(shop_id)
    for i in range(1, int(sys.argv[1])+1):
        try:
            url = "http://www.dianping.com/search/keyword/10/0_bianlidian/%sp%s" % (sys.argv[2], str(i))
            print(url)
            resource = get_html(url)
            resource = resource.replace("&nbsp; ", "")
            soup = BeautifulSoup(resource, 'lxml')
            shop_list = soup.find_all('a', {"class": "o-favor J_o-favor"})
            shop_id_list = [shop.attrs["data-shopid"] for shop in shop_list]
            print(shop_id_list)
            if len(shop_id_list) == 0:
                print(resource)

            for shop_id in shop_id_list:
                try:
                    if shop_id in db_shop_ids:
                        continue
                    url = "http://www.dianping.com/shop/%s" % shop_id
                    print(url)
                    resource = get_html(url)
                    resource = resource.replace("&nbsp; ", "")
                    soup = BeautifulSoup(resource, 'lxml')
                    # 店铺名称
                    shopname = get_shop_name(soup)
                    # 地址
                    address = get_address(soup)
                    # 根据地址获取坐标
                    lng, lat, district = get_location(address)
                    # 手机号
                    telphonenumber = ""
                    try:
                        telphonenumber = get_telphonenumber(soup)
                    except Exception as e:
                        print(str(e))
                    # 价格
                    avgprice = get_avgprice(soup)
                    # 营业时间
                    open_time = get_open_time(soup)
                    print("完成")
                    # 插入数据库
                    sql = "INSERT INTO dianping_info(name, avgprice, telphone, address, lng, lat, district, open_time, shop_id, district_code, page, created_at) " \
                          "VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', current_timestamp)" \
                          % (shopname, avgprice, telphonenumber, address, lng, lat, district, open_time, shop_id, sys.argv[2], i)
                    cursor.execute(sql)
                    db.commit()
                except Exception as e:
                    db.rollback()
                    print(str(e))
        except Exception as e:
            print(str(e))
except Exception as e:
    db.rollback()
    print(str(e))
finally:
    db.close()
    print("end")


