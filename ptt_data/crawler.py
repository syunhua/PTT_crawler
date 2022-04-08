import datetime
import sys
import time
import typing
import re
from lxml import etree
import pandas as pd
import requests, bs4
from loguru import logger
from pydantic import BaseModel
from tqdm import tqdm,trange
from Router import Router

def ptt_crawler(page, board='Gossiping'):
    try:
        url = f'https://www.ptt.cc/bbs/{board}/index{page}.html'
        response = requests.get(url, cookies={'over18':'1'})

        # 解析內容 (轉為string)
        content = response.content.decode()
        # 把string轉為hmtl node tree，回傳根節點
        html = etree.HTML(content)

        # 文章標題
        title = html.xpath("//div[@class='r-ent']/div[@class='title']/a[@href]/text()")
        #push = html.xpath("//div[@class='nrec']/span/text()")

        # 文章url
        title_url_o = html.xpath("//div[@class='r-ent']/div[@class='title']//@href")
        title_url=[]
        for i in title_url_o:
            title_url_i = f'https://www.ptt.cc{i}'
            title_url.append(title_url_i)

        # 各個文章內容
        sub_response=[]
        sub_content=[]
        sub_html=[]
        for sub_url in title_url:
            sub_response.append(requests.get(sub_url, cookies={'over18':'1'}))
        for s_r in sub_response:
            sub_content.append(s_r.content.decode())
        for s_c in sub_content:
            sub_html.append(etree.HTML(s_c))

        article_content_o=[]
        article_content_o_null=[]
        author_o=[]
        date_o=[]
        push_id=[]
        push_content=[]
        push_ip_time=[]
        push_good=[]
        push_bad=[]
        for s_h in sub_html:
            article_content_o.append(s_h.xpath("//div[@id='main-content']/text()"))
            article_content_o_null.append(s_h.xpath("(//div[@id='main-content']/text())[2]"))
            author_o.append(s_h.xpath("(//span[@class='article-meta-value'])[1]/text()"))
            date_o.append(s_h.xpath("(//span[@class='article-meta-value'])[4]/text()"))
            push_id.append(s_h.xpath("//div[@class='push']/span[@class='f3 hl push-userid']/text()"))
            push_content.append(s_h.xpath("//div[@class='push']/span[@class='f3 push-content']/text()"))
            push_ip_time.append(s_h.xpath("//div[@class='push']/span[@class='push-ipdatetime']/text()"))
            push_good.append(s_h.xpath("//span[@class='hl push-tag']/text()"))
            push_bad.append(s_h.xpath("//span[@class='f1 hl push-tag']/text()"))


        # 推文ID+推文內容+推文IP與時間
        push_all_o = []

        for i,j,k in zip(push_id,push_content,push_ip_time):
            push_all_o.append([i,j,k])

        # 文章推文的表
        #push_c = {'推文ID':push_id, '推文內容':push_content, '推文IP與時間':push_ip_time, 'ALL':push_all_o}
        #push_parse = pd.DataFrame(push_c) 

        # 去括號
        article_content=[]
        article_content_null=[]
        author=[]
        date=[]
        push_all=[]
        good_bad=[]
        for i in range(len(title)):
            try:
                article_content_null.append(article_content_o_null[i][0])
            except:
                article_content_null.append('')
            article_content.append(article_content_o[i][0])
            author.append(author_o[i][0])
            date.append(date_o[i][0])
            push_all.append(push_all_o[i][0])
            good_bad.append(len(push_good[i])-len(push_bad[i]))

        article_content = [re.sub('\n', '', article_content[i]) for i in range(len(article_content))]
        article_content_null = [re.sub('\n', '', article_content_null[i]) for i in range(len(article_content_null))]

        # 時間分割
        date = [re.split(' ', date[i]) for i in range(len(date))]
        Day_of_week=[]
        Month_Days=[]
        Times=[]
        year=[]
        for i in range(len(date)):
            Day_of_week.append(date[i][0])
            Month_Days.append(date[i][1]+'/'+date[i][2])
            Times.append(date[i][3])
            year.append(date[i][4])

        # 表格
        ptt_parse = pd.DataFrame({'文章標題':title,
                                  '推噓':good_bad,
                                  '作者':author,
                                  '年':year,
                                  '月/日':Month_Days,
                                  '星期':Day_of_week,
                                  '時間':Times,
                                  '文章內容':article_content,'文章內容2':article_content_null,
                                  '推文內容':push_all,
                                  '連結':title_url})

        ptt_parse['文章內容'] = ptt_parse['文章內容']+' '+ptt_parse['文章內容2']
        del ptt_parse['文章內容2']

        

        return ptt_parse
    
    except:
        print(f"{page} error")


def gen_task_paramter_list(start_page, end_page ):
    """建立時間列表, 用於爬取所有資料, 這時有兩種狀況
    1. 抓取歷史資料
    2. 每日更新
    因此, 爬蟲日期列表, 根據 history 參數進行判斷
    """
    pages = ( int(end_page) - int(start_page) ) + 1
    parameter_list = [ str(start_page + p) for p in range(pages) ]
    return parameter_list



def ptt_concat(start_page, end_page):
    df = pd.concat([ptt_crawler(p, board='Gossiping') for p in gen_task_paramter_list(start_page, end_page)])
    #ptt_data = '/Users/syunhua/Desktop/Side_project/ptt_data/crawler_ptt_data.csv'
    #df.to_csv( ptt_data  , index=False )
    #for i in trange(int(start_page),int(end_page))]).reset_index(drop=True)
    return df


def main( start_page, end_page):
    pages = ( int(end_page) - int(start_page) ) + 1
    date_list = [ str(start_page + p) for p in range(pages) ]
    db_router = Router()
    for page in tqdm(date_list):
        logger.info(page)
        df = ptt_crawler(page=page)
        print(df)



if __name__=="__main__":
    start_page,end_page = sys.argv[1:]
    main(start_page, end_page)
