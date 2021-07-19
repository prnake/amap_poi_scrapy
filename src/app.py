#! /usr/bin/env python3
# -*- coding:utf-8 -*-
import os
import re
import json
import time
import argparse
from threading import Thread, Lock
from queue import Queue
from urllib.parse import quote
from urllib import request
import pandas
from transCoordinateSystem import gcj02_to_wgs84, gcj02_to_bd09
from area_code import area_code

parser = argparse.ArgumentParser()
parser.add_argument('-v', '--verbose', help="打印更多的信息", action="store_true")
parser.add_argument('-r', '--reset', help="忽略当前存在的配置信息", action="store_true")

amap_key_lock = Lock()
all_pois_lock = Lock()
all_pois = []
scrapy_id = []
all_pois_count = 0
all_pois_write_count = 0

amap_pos_text_url = "https://restapi.amap.com/v3/place/text?key={}&keywords={}&city={}&citylimit=true&offset=25&page={}&output=json"
amap_pos_poly_url = "https://restapi.amap.com/v3/place/polygon?key={}&keywords={}&polygon={}&citylimit=true&offset=25&page={}&output=json"
amap_district_url = "https://restapi.amap.com/v3/config/district?subdistrict=2&extensions=all&key={}"


def request_with_key(url):
    global amap_web_key
    amap_key_lock.acquire()
    req_url = url.format(amap_web_key)
    amap_key_lock.release()
    if args.verbose:
        print('请求url:', req_url)
    with request.urlopen(req_url) as f:
        data = f.read()
        data = data.decode('utf-8')
    data = json.loads(data)
    while int(data["status"]) != 1:
        print(data)
        if int(data["infocode"]) in [10001, 10044]:
            amap_key_lock.acquire()
            req_url = url.format(amap_web_key)
            if args.verbose:
                print('请求url:', req_url)
            with request.urlopen(req_url) as f:
                data = f.read()
                data = data.decode('utf-8')
            data = json.loads(data)
            if int(data["infocode"]) in [10001,10044]:
                amap_web_key = input("已超出使用额度或密钥不正确，请输入新Key：")
                req_url = url.format(amap_web_key)
            amap_key_lock.release()
        else:
            print("请求被拒绝，可能触发QPS限制，5s后重试")
            time.sleep(5)
            if args.verbose:
                print('请求url:', req_url)
            with request.urlopen(req_url) as f:
                data = f.read()
                data = data.decode('utf-8')
            data = json.loads(data)
        
    
    return data

# 根据城市名称和分类关键字获取poi数据
def getpois(url):
    page = 1
    poilist = []
    while True:
        result = getpoi_page(url, page)
        if result['count'] == '0':
            break

        pois = result['pois']
        for i in range(len(pois)):
            poilist.append(pois[i])

        page += 1
    return poilist

# 数据写入csv文件中


def write_to_csv(poilist, file_name):
    data_csv = {}
    lons, lats, names, addresss, pnames, citynames, business_areas, types = [
    ], [], [], [], [], [], [], []

    for i in range(len(poilist)):
        location = poilist[i].get('location')
        name = poilist[i].get('name')
        address = poilist[i].get('address')
        pname = poilist[i].get('pname')
        cityname = poilist[i].get('cityname')
        business_area = poilist[i].get('business_area')
        type = poilist[i].get('type')
        lng = str(location).split(",")[0]
        lat = str(location).split(",")[1]

        if (coord == 2):
            result = gcj02_to_wgs84(float(lng), float(lat))
            lng = result[0]
            lat = result[1]
        if (coord == 3):
            result = gcj02_to_bd09(float(lng), float(lat))
            lng = result[0]
            lat = result[1]
        lons.append(lng)
        lats.append(lat)
        names.append(name)
        addresss.append(address)
        pnames.append(pname)
        citynames.append(cityname)
        if business_area == []:
            business_area = ''
        business_areas.append(business_area)
        types.append(type)
    data_csv['lon'], data_csv['lat'], data_csv['name'], data_csv['address'], data_csv['pname'], \
        data_csv['cityname'], data_csv['business_area'], data_csv['type'] = \
        lons, lats, names, addresss, pnames, citynames, business_areas, types

    df = pandas.DataFrame(data_csv)

    file_path = 'data' + os.sep + file_name + os.sep + file_name + ".csv"

    df.to_csv(file_path, index=False, encoding='utf_8_sig')
    return file_path


# 单页获取pois
def getpoi_page(url, page):
    req_url = url.format("{}",page)
    data = request_with_key(req_url)
    return data


def get_area_list(code):
    '''
    获取城市的所有区域，只在指定了非全国的地区时会用到
    '''
    if args.verbose:
        print('获取城市的所有区域：code: ' + str(code).strip())
    data = get_distrinctNoCache(code)

    districts = data['districts'][0]['districts']
    # 判断是否是直辖市: 北京市、上海市、天津市、重庆市
    if (code.startswith('重庆') or code.startswith('上海') or code.startswith('北京') or code.startswith('天津')):
        districts = data['districts'][0]['districts'][0]['districts']

    area = []
    for district in districts:
        area.append(district['adcode'])

    if args.verbose:
        print(area)

    return area


def get_distrinctNoCache(code):
    '''
    获取中国城市行政区划，只在指定了非全国的地区时会用到
    '''
    global amap_district_url
    url = amap_district_url + "&keywords=" + quote(code)

    data = request_with_key(url)

    return data


def divide_pos_scrapy(url,min_x, min_y, max_x, max_y):
    '''
    每次区域划分为2X2，直到每个区域内的数量小于800为止
    '''
    pos_scrapy = []
    for j in range(2):
        for k in range(2):
            pos = [round(max_x*j/2+min_x*(2-j)/2, 6), round(max_y*k/2+min_y*(2-k)/2, 6), round(
                max_x*(j+1)/2+min_x*(2-j-1)/2, 6), round(max_y*(k+1)/2+min_y*(2-k-1)/2, 6)]
            pos_url = url.format("{}",
                quote("{},{}|{},{}".format(*pos)), "{}")
            result = getpoi_page(pos_url, 1)
            count = int(result["count"])
            if count > 800:
                pos_scrapy.extend(divide_pos_scrapy(url,*pos))
            elif count > 0:
                pos_scrapy.append(["{},{}|{},{}".format(*pos), pos_url,count])

    return pos_scrapy


def gen_pos_scrapy(url,code):
    '''
    当一个区域的店铺数量大于800时，实际值可能更大，使用多边形API进一步估计
    '''
    data = get_distrinctNoCache(code)
    # 这个API可能为空
    try:
        polyline = data["districts"][0]["polyline"]
    except:
        return None
    poly_list = re.split("[;\|]", polyline)
    x_list,y_list = [],[]
    for poly in poly_list:
        if not poly:
            continue
        try:
            x, y = poly.split(",")
        except:
            pass
        x,y = float(x),float(y)
        x_list.append(x)
        y_list.append(y)

    min_x, min_y = min(x_list), min(y_list)
    max_x, max_y = max(x_list), max(y_list)

    pos_scrapy = divide_pos_scrapy(url,min_x, min_y, max_x, max_y)

    return pos_scrapy


def queue_get_scrapy_list(q, scrapy_list,scrapy_list_lock):
    '''
    get_scrapy_list 函数的并发部分
    '''
    global keywords, amap_pos_text_url, amap_pos_poly_url
    while not q.empty():
        area = q.get()
        url = amap_pos_text_url.format(
            "{}", quote(keywords), quote(area), "{}")
        result = getpoi_page(url, 1)
        count = int(result["count"])
        if count > 800:
            pos_url = amap_pos_poly_url.format(
                "{}", quote(keywords), "{}", "{}")
            pos_scrapy = gen_pos_scrapy(pos_url, area)
            if pos_scrapy:
                scrapy_list_lock.acquire()
                scrapy_list.extend(pos_scrapy)
                scrapy_list_lock.release()
            else:
                scrapy_list_lock.acquire()
                scrapy_list.append([area, url, count])
                scrapy_list_lock.release()
        elif count > 0:
            scrapy_list_lock.acquire()
            scrapy_list.append([area, url, count])
            scrapy_list_lock.release()

def get_scrapy_list():
    '''
    获取需要爬取的 url，采用地理区域 API 为主，多边形 API 辅助的方法
    '''
    global keywords,types
    area_list = []
    if "全国" in city:
        for province in area_code:
            for ct in province["cities"]:
                if len(ct["districts"]) > 0:
                    for dt in ct["districts"]:
                        area_list.append(dt["code"])
                else:
                    area_list.append(ct["code"])
    else:
        for ct in city:
            area_list.extend(get_area_list(ct))

    area_q = Queue()
    scrapy_list = []
    scrapy_list_lock = Lock()

    for area in area_list:
        area_q.put(area)
    
    area_threads = [Thread(target=queue_get_scrapy_list, args=(area_q, scrapy_list, scrapy_list_lock,))
               for _ in range(thread_num)]

    for thread in area_threads:
        thread.start()
    for thread in area_threads:
        thread.join()
    
    if args.verbose:
        print(scrapy_list)

    with open(f"{folder_path}scrapy_list.json", "w", encoding='utf-8') as f:
        json.dump(scrapy_list, f, ensure_ascii=False)


def queue_scrapy(q):
    '''
    每次从队列 q 中获取一个 url,爬取后放入 all_pois
    '''
    global all_pois_lock, all_pois_write_count, all_pois_count, scrapy_id
    while not q.empty():
        id, url = q.get()
        pois_area = getpois(url)
        all_pois_lock.acquire()
        all_pois.extend(pois_area)
        all_pois_write_count += 1
        scrapy_id.append(id)
        if all_pois_write_count % 100 ==0:
            with open(f"{folder_path}results.json", "w", encoding='utf-8') as f:
                json.dump(all_pois, f, ensure_ascii=False)
            with open(f"{folder_path}scrapy_id.json", "w", encoding='utf-8') as f:
                json.dump(scrapy_id, f, ensure_ascii=False)
            print("当前进度:", f"{all_pois_write_count}/{all_pois_count}")
        all_pois_lock.release()

if __name__ == "__main__":
    args = parser.parse_args()

    if not args.reset and os.path.exists("config.json"):
        print("config.json 文件存在，配置自动读取，如需要修改请删除 config.json 后重试")
    else:
        print("配置创建中")
        config = {}
        config["thread_num"] = int(input("并发线程数（推荐5-10，高德个人开发者QPS为50）:"))
        config["amap_web_key"] = input("高德开发者key:")
        config["city"] = input("输入城市，多个请用\",\"分隔，完整查询请输入\"全国\":").split(",")
        url_type = int(input("基于关键词查询请输入0，基于POI分类编码查询请输入1:"))
        if url_type == 0:
            config["keywords"] = input(
                "输入搜索关键词，多个请用\"|\"分隔，例如\"奶茶|咖啡\"用于查询所有的奶茶店和咖啡店:")
        else:
            print("基于POI分类编码查询,可输入分类代码或汉字")
            print("若用汉字，请严格按照 amap_poicode.xlsx 之中的汉字填写")
            config["types"] = input("输入分类代码或汉字:")
        config["coord"] = int(
            input("选择输出数据坐标系,1为高德GCJ20坐标系，2WGS84坐标系，3百度BD09坐标系（推荐2）:"))
        with open("config.json", "w", encoding='utf-8') as f:
            config = json.dump(config, f, sort_keys=True,
                               indent=4, separators=(',', ':'), ensure_ascii=False)
            print("配置已写入 config.json")

    
    with open("config.json", "r", encoding='utf-8') as f:
            config = json.load(f)

    thread_num = int(config["thread_num"])
    amap_web_key = config["amap_web_key"]
    city = config["city"]
    if "keywords" in config:
        keywords = config["keywords"]
    elif "types" in config:
        keywords = config["types"]
        amap_pos_text_url = amap_pos_text_url.replace("keywords","types")
        amap_pos_poly_url = amap_pos_poly_url.replace("keywords", "types")
    coord = int(config["coord"])

    file_name = "-".join(keywords.split("|")) + '-' + "-".join(city)
    folder_path = 'data' + os.sep + file_name + os.sep
    if os.path.exists(folder_path) is False:
        os.makedirs(folder_path)

    print("配置读取成功")

    if os.path.exists(f"{folder_path}scrapy_list.json"):
        if input(f"{folder_path}scrapy_list.json 文件存在，是否继续上一次查询(Y/N)?") != "Y":
            get_scrapy_list()
        else:
            if os.path.exists(f"{folder_path}results.json"):
                with open(f"{folder_path}results.json", "r", encoding='utf-8') as f:
                    all_pois = json.load(f)
            if os.path.exists(f"{folder_path}scrapy_id.json"):
                with open(f"{folder_path}scrapy_id.json", "r", encoding='utf-8') as f:
                    scrapy_id = json.load(f)
    else:
        print("需要爬取的url中，预计需要2分钟")
        get_scrapy_list()
    
    print("查询的地区:", city)
    print("查询的关键词:", keywords)
    
    with open(f"{folder_path}scrapy_list.json", "r", encoding='utf-8') as f:
        scrapy_list = json.load(f)
    
    q = Queue()

    id, num_cnt, req_cnt = 0, 0, 0
    for area, url, count in scrapy_list:
        num_cnt += count
        if id not in scrapy_id:
            req_cnt += count//25
            q.put([id,url])
        id += 1
    
    all_pois_count = q.qsize()
    
    print("总数量约"+str(num_cnt), "预计请求"+str(req_cnt)+"次", "预计需要"+str(round(req_cnt/thread_num/60,1))+"分钟")

    print("开始查询")

    threads = [Thread(target=queue_scrapy, args=(q,))
               for _ in range(thread_num)]
    
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
        
    file_path = write_to_csv(all_pois, file_name)

    print("数据汇总，总数为:", len(all_pois))
    print("文件保存至", file_path)
    input("按下回车结束程序...")
