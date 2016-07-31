# coding: utf-8
'''
Generate feeds for elasticsearch
schema: ../elasticsearch/schema
curl -s -XPOST localhost:9200/_bulk --data-binary @requests
cat requests

'''

from datetime import datetime
import os
import json

from bs4 import BeautifulSoup
from process import ListingProcessor, PostProcessor
import utils


def post_process(link, raw_meta, content, images, pushes):
    thumbups = 0
    article_time = datetime.strptime(raw_meta[u'時間'], '%a %b %d %H:%M:%S %Y')
    update_time = datetime.now()
    
    processed_imgs = []
    for img in images:
        processed_imgs.append({"link": img, "views":0})

    processed_pushes = []
    for push in pushes:
        push_dic = {
            "userid": push["userid"],
            "content": push["content"],
            "method": push["tag"],
            "date": datetime.strptime(push["ipdatetime"], '%m/%d %H:%M').replace(year=article_time.year).isoformat()
        }
        processed_pushes.append(push_dic)
        if push_dic["method"] == u'推':
            thumbups += 1
        elif push_dic["method"] == u'噓':
            thumbups -= 1

    dic = {}
    dic["author"] = raw_meta[u'作者'].split(' ')[0] # "id (nickname)" --> "id"
    dic["article_time"] = article_time.isoformat()
    dic["update_time"] = update_time.isoformat()
    dic["link"] = link
    dic["board"] = raw_meta[u'看板']
    dic["title"] = raw_meta[u'標題']
    dic["content"] = content.strip()
    dic["images"] = processed_imgs
    dic["pushes"] = processed_pushes
    dic["thumbups"] = thumbups
    dic["historical_thumbups"] = [{"thumbups":thumbups, "date":update_time.isoformat()}]
    dic["views"] = 0

    return dic


if __name__ == '__main__':
    LIST_MAX = 1883
    LIST_MIN = 0
    LOCAL_DUMP_PATH = '../data/'
    index_meta_dic = {'index': {'_index': 'gulis', '_type': 'article'}}

    for num in xrange(LIST_MAX, LIST_MIN, -1):
        list_file = os.path.join(LOCAL_DUMP_PATH, 'list', 'index'+str(num)+'.html')

        ### process list
        soup = BeautifulSoup(open(list_file), 'lxml')

        listing = ListingProcessor(soup)

        try:
            raw_post_info_list = listing.collect_raw_post_info_list()
        except:
            continue

        for raw_post_info in raw_post_info_list:

            # link: https://www.ptt.cc/bbs/Beauty/M.1409553672.A.301.html
            link = raw_post_info['link']

            page_url = utils.get_page_url(link)

            # process post from local
            try:
                soup = BeautifulSoup(open(os.path.join(LOCAL_DUMP_PATH, 'post', page_url)), 'lxml')
            except:
                continue
            post = PostProcessor(soup)

            raw_meta = post.collect_raw_meta()
            content = post.collect_content()
            images = post.collect_images()
            pushes = post.collect_pushs()

            # skip no imgs for now
            if len(images) == 0:
                continue

            try:
                feed_dic = post_process(link, raw_meta, content, images, pushes)
            except:
                continue
            print json.dumps(index_meta_dic)
            print json.dumps(feed_dic)