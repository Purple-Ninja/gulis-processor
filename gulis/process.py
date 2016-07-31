# coding: utf-8

import requests
from urlparse import urljoin
from collections import namedtuple
import json
import bs4
import os
import utils
from bs4 import BeautifulSoup
from itertools import izip

class Processor(object):
    def __init__(self, soup):
        self.soup = soup

    def get_text(self, element):
        return element.text.strip()

class PostProcessor(Processor):
    """
    docstring for PostProcessor
    """
    def __init__(self, soup):
        super(PostProcessor, self).__init__(soup)

    def collect_raw_meta(self):
        """
        { 
          '作者': 'gaiaesque (一起來浸水桶吧)',
          '看板': 'Beauty',
          '標題': '[正妹] 平祐奈 甜美正妹',
          '時間': 'Thu Oct 22 22:30:31 2015'
        }
        """
        keys = map(self.get_text, self.soup.select('#main-content .article-meta-tag'))
        vals = map(self.get_text, self.soup.select('#main-content .article-meta-value'))

        return {} if len(keys) != len(vals) else dict(zip(keys, vals))

    def collect_content(self):
        """
        find and concatenate unwrapped strings
        """
        return ''.join(filter(lambda x:type(x) == bs4.element.NavigableString, self.soup.select_one('#main-content').contents))

    def collect_images(self, protocol='http'):
        """
        [
            'http://i.imgur.com/ijXPYqV.jpg',
            'http://i.imgur.com/eMwxfm2.jpg',
            'http://i.imgur.com/7zcr6vq.jpg'
        ]
        """
        imgs = self.soup.select('.richcontent img')
        return map(lambda img: '' if 'src' not in img.attrs else protocol+':'+img.attrs['src'], imgs)

    def collect_pushs(self):
        """
        {
            'content': u'竟然在表特看到',
            'tag': u'推',
            'userid': u'LemonUrsus',
            'ipdatetime': u' 09/01 22:13'
        }
        """
        keys = ('tag', 'userid', 'content', 'ipdatetime')

        tags = map(self.get_text, self.soup.select('.push .push-tag'))
        userids = map(self.get_text, self.soup.select('.push .push-userid'))
        contents = map(self.get_text, self.soup.select('.push .push-content'))
        ipdatetimes = map(self.get_text, self.soup.select('.push .push-ipdatetime'))

        return [ dict(zip(keys, push)) for push in izip(tags, userids, contents, ipdatetimes) ]

class ListingProcessor(Processor):

    def __init__(self, soup):
        super(ListingProcessor, self).__init__(soup)

    def collect_raw_post_info_list(self):
        """
        {
            'author': u'MissBB',
            'date': u' 9/02',
            'link': 'https://www.ptt.cc/bbs/Beauty/M.1409646948.A.F45.html',
            'mark': u'',
            'push': u'21',
            'title': u'[神人] 宮原眼科冰淇淋女孩'
        }
        """
        sections = self.soup.select('.r-ent')
        keys = ['push', 'mark', 'title', 'date', 'author']
        classNames = ['.nrec', '.mark', '.title a', '.meta .date', '.meta .author']
        post_info_list = []
        for section in sections:
            info = dict(zip(keys, (section.select_one(className).text for className in classNames)))
            info['link'] = urljoin('https://www.ptt.cc', section.select_one('.title a').attrs['href'])

            # artifact
            # TODO: convert date to datetime object
            # TODO: convert mark to boolean
            # TODO: convert push to integer

            post_info_list.append(info)

        return post_info_list

    def collect_prev_btn_link(self):
        prev_btn = self.soup.select('.btn-group-paging .btn')[2]
        prev_btn_link = urljoin('https://www.ptt.cc', prev_btn.attrs['href'])
        return prev_btn_link

if __name__ == '__main__':

    LOCAL_DUMP = True
    LOCAL_DUMP_PATH = '../data/post/'

    ### process list
    soup = BeautifulSoup(open('../data/list/index1217.html'), 'lxml')

    listing = ListingProcessor(soup)

    raw_post_info_list = listing.collect_raw_post_info_list()
    prev_btn_link = listing.collect_prev_btn_link()

    for raw_post_info in raw_post_info_list:

        # link: https://www.ptt.cc/bbs/Beauty/M.1409553672.A.301.html
        link = raw_post_info['link']

        # if(LOCAL_DUMP) {
        #     path = os.path.join(LOCAL_DUMP_PATH, os.path.split(link)[1])
        #     soup = BeautifulSoup(open(path), 'html')
        # } else {
        #     soup = BeautifulSoup(crawl(link))
        # }
        print '> link:', link

        page_url = utils.get_page_url(link)

        ### process post
        soup = BeautifulSoup(open(os.path.join('..', 'data', 'post', page_url)), 'lxml')

        post = PostProcessor(soup)

        raw_meta = post.collect_raw_meta()
        content = post.collect_content()
        images = post.collect_images()
        pushes = post.collect_pushs()

        if len(images):
            print '>> raw_meta', json.dumps(raw_meta, indent=2)
            print '>> content', json.dumps(content, indent=2)
            print '>> images', json.dumps(images, indent=2)
            print '>> pushes', json.dumps(pushes, indent=2)
            raw_input()
