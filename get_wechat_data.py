# -*- coding: utf-8 -*-
"""
Created on Wed Apr 26 11:27:32 2017

@author: Administrator
"""

import requests,json,time,datetime,sys
import MySQLdb as mdb

session=requests.Session()
config={
        'host':'127.0.0.1',
        'port':3306,
        'user':'root',
        'passwd':'root',
        'db':'wechat_data',
        'charset':'utf8'
        }
conn=mdb.connect(**config)
cur=conn.cursor()

def get_account_data(tablename):
    cur.execute('select * from %s'%tablename)
    account_data=cur.fetchall()
    return account_data

def get_access_token(appid,secretkey):
    access_token_url='https://api.weixin.qq.com/cgi-bin/token?grant_type=client_credential&appid=%s&secret=%s'%(appid,secretkey)
    response=session.get(access_token_url)
    jsonstr=response.text
    jsonstr=json.loads(jsonstr)
    access_token=jsonstr['access_token']
    print 'have got access_token' 
    return access_token

def set_query_date(days):
    today=datetime.date.today()
    query_date=today-datetime.timedelta(days=days)
    query_date=str(query_date)
    return query_date

###########################用户分析数据接口###########################
def get_usersummary_data(wechat_id,wechat_name,access_token,begin_date,end_date):
    getusersummary_url='https://api.weixin.qq.com/datacube/getusersummary?access_token=%s'%access_token
    postdata={
            'begin_date': begin_date, 
            'end_date'  : end_date
            }
    postdata = json.dumps(postdata)#转成str格式,不然会报格式不对的错
    usersummary_response=session.post(getusersummary_url,data=postdata)    
    usersummary_json=usersummary_response.text    
    usersummary_json=json.loads(usersummary_json)
    usersummary_list=usersummary_json['list']
    for i in range(0,len(usersummary_list)):
        user_source=usersummary_list[i]['user_source']
        new_user=usersummary_list[i]['new_user']
        cancel_user=usersummary_list[i]['cancel_user']
        ref_date=usersummary_list[i]['ref_date']
        usersummary_value=[wechat_id,wechat_name,ref_date,user_source,new_user,cancel_user]
        cur.execute('insert into usersummary_data values (%s,%s,%s,%s,%s,%s)',usersummary_value)
    conn.commit()
    
def get_usercumulate_data(wechat_id,wechat_name,access_token,begin_date,end_date):  
    getusercumulate_url='https://api.weixin.qq.com/datacube/getusercumulate?access_token=%s'%access_token
    postdata={
            'begin_date': begin_date, 
            'end_date'  : end_date
            }
    postdata = json.dumps(postdata)#转成str格式,不然会报格式不对的错
    usercumulate_response=session.post(getusercumulate_url,data=postdata)
    usercumulate_json=usercumulate_response.text
    usercumulate_json=json.loads(usercumulate_json)
    usercumulate_list=usercumulate_json['list']
    cumulate_user=usercumulate_list[0]['cumulate_user']
    ref_date=usercumulate_list[0]['ref_date']
    usercumulate_value=[wechat_id,wechat_name,ref_date,cumulate_user]
    cur.execute('insert into usercumulate_data values (%s,%s,%s,%s)',usercumulate_value)
    conn.commit()

###########################图文分析数据接口###########################    	
def get_articlesummary_data(wechat_id,wechat_name,access_token,begin_date,end_date):
    postdata={
            'begin_date': begin_date, 
            'end_date'  : end_date
            }
    postdata = json.dumps(postdata)#转成str格式,不然会报格式不对的错
    articlesummaryurl='https://api.weixin.qq.com/datacube/getarticlesummary?access_token=%s'%access_token
    articlesummary_response=session.post(articlesummaryurl,data=postdata)
    articlesummary_json=articlesummary_response.text
    articlesummary_json=json.loads(articlesummary_json)
    articlesummary_list=articlesummary_json['list']
    for i in range(0,len(articlesummary_list)):
        ref_date=articlesummary_list[i]['ref_date']
        user_source=articlesummary_list[i]['user_source']
        msgid=articlesummary_list[i]['msgid']
        title=articlesummary_list[i]['title']
        int_page_read_user=articlesummary_list[i]['int_page_read_user']
        int_page_read_count=articlesummary_list[i]['int_page_read_count']
        ori_page_read_user=articlesummary_list[i]['ori_page_read_user']
        ori_page_read_count=articlesummary_list[i]['ori_page_read_count']
        share_user=articlesummary_list[i]['share_user']
        share_count=articlesummary_list[i]['share_count']
        add_to_fav_user=articlesummary_list[i]['add_to_fav_user']
        add_to_fav_count=articlesummary_list[i]['add_to_fav_count']
        articlesummary_value=[wechat_id,wechat_name,ref_date,user_source,msgid,title,int_page_read_user,int_page_read_count,ori_page_read_user,ori_page_read_count,share_user,share_count,add_to_fav_user,add_to_fav_count]
        cur.execute('insert into articlesummary_data value(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',articlesummary_value)
    conn.commit()
    
def get_articletotal_data(table_name,wechat_id,wechat_name,access_token,begin_date,end_date):
    postdata={
            'begin_date': begin_date, 
            'end_date'  : end_date
            }
    postdata = json.dumps(postdata)#转成str格式,不然会报格式不对的错
    articletotalurl='https://api.weixin.qq.com/datacube/getarticletotal?access_token=%s'%access_token
    articletotal_response=session.post(articletotalurl,data=postdata)
    articletotal_json=articletotal_response.text
    articletotal_json=json.loads(articletotal_json)
    articletotal_list=articletotal_json['list']
    for i in range(0,len(articletotal_list)):
        ref_date=articletotal_list[i]['ref_date']
        user_source=articletotal_list[i]['user_source']
        msgid=articletotal_list[i]['msgid']
        title=articletotal_list[i]['title']
        for j in range(0,len(articletotal_list[i]['details'])):
            stat_date=articletotal_list[i]['details'][j]['stat_date']
            target_user=articletotal_list[i]['details'][j]['target_user']
            int_page_read_user=articletotal_list[i]['details'][j]['int_page_read_user']
            int_page_read_count=articletotal_list[i]['details'][j]['int_page_read_count']
            ori_page_read_user=articletotal_list[i]['details'][j]['ori_page_read_user']
            ori_page_read_count=articletotal_list[i]['details'][j]['ori_page_read_count']
            share_user=articletotal_list[i]['details'][j]['share_user']
            share_count=articletotal_list[i]['details'][j]['share_count']
            add_to_fav_user=articletotal_list[i]['details'][j]['add_to_fav_user']
            add_to_fav_count=articletotal_list[i]['details'][j]['add_to_fav_count']
            int_page_from_session_read_user=articletotal_list[i]['details'][j]['int_page_from_session_read_user']
            int_page_from_session_read_count=articletotal_list[i]['details'][j]['int_page_from_session_read_count']
            int_page_from_other_read_user=articletotal_list[i]['details'][j]['int_page_from_other_read_user']
            int_page_from_other_read_count=articletotal_list[i]['details'][j]['int_page_from_other_read_count']
            int_page_from_hist_msg_read_user=articletotal_list[i]['details'][j]['int_page_from_hist_msg_read_user']
            int_page_from_hist_msg_read_count=articletotal_list[i]['details'][j]['int_page_from_hist_msg_read_count']
            int_page_from_friends_read_user=articletotal_list[i]['details'][j]['int_page_from_friends_read_user']
            int_page_from_friends_read_count=articletotal_list[i]['details'][j]['int_page_from_friends_read_count']
            int_page_from_feed_read_user=articletotal_list[i]['details'][j]['int_page_from_feed_read_user']
            int_page_from_feed_read_count=articletotal_list[i]['details'][j]['int_page_from_feed_read_count']
            feed_share_from_session_user=articletotal_list[i]['details'][j]['feed_share_from_session_user']
            feed_share_from_session_cnt=articletotal_list[i]['details'][j]['feed_share_from_session_cnt']
            feed_share_from_other_user=articletotal_list[i]['details'][j]['feed_share_from_other_user']
            feed_share_from_other_cnt=articletotal_list[i]['details'][j]['feed_share_from_other_cnt']
            feed_share_from_feed_user=articletotal_list[i]['details'][j]['feed_share_from_feed_user']
            feed_share_from_feed_cnt=articletotal_list[i]['details'][j]['feed_share_from_feed_cnt']
            articletotal_value=[wechat_id,wechat_name,ref_date,user_source,msgid,title,stat_date,target_user,int_page_read_user,int_page_read_count,ori_page_read_user,ori_page_read_count,share_user,share_count,add_to_fav_user,add_to_fav_count,int_page_from_session_read_user,int_page_from_session_read_count,int_page_from_other_read_user,int_page_from_other_read_count,int_page_from_hist_msg_read_user,int_page_from_hist_msg_read_count,int_page_from_friends_read_user,int_page_from_friends_read_count,int_page_from_feed_read_user,int_page_from_feed_read_count,feed_share_from_session_user,feed_share_from_session_cnt,feed_share_from_other_user,feed_share_from_other_cnt,feed_share_from_feed_user,feed_share_from_feed_cnt]
            sql='insert into %s'%table_name
            cur.execute(sql+' value(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',articletotal_value)
    conn.commit()

###########################消息分析数据接口###########################    
def get_upstreammsg_data(wechat_id,wechat_name,access_token,begin_date,end_date):
    postdata={
            'begin_date': begin_date, 
            'end_date'  : end_date
            }
    postdata = json.dumps(postdata)#转成str格式,不然会报格式不对的错
    upstreammsgurl='https://api.weixin.qq.com/datacube/getupstreammsg?access_token=%s'%access_token
    upstreammsg_response=session.post(upstreammsgurl,data=postdata)
    upstreammsg_json=upstreammsg_response.text
    upstreammsg_json=json.loads(upstreammsg_json)
    upstreammsg_list=upstreammsg_json['list']
    for i in range(0,len(upstreammsg_list)):
        ref_date=upstreammsg_list[i]['ref_date']
        msg_type=upstreammsg_list[i]['msg_type']
        msg_user=upstreammsg_list[i]['msg_user']
        msg_count=upstreammsg_list[i]['msg_count']
        upstreammsg_value=[wechat_id,wechat_name,ref_date,msg_type,msg_user,msg_count]
        cur.execute('insert into upstreammsg_data values (%s,%s,%s,%s,%s,%s)',upstreammsg_value)
    conn.commit()    

def get_upstreammsghour_data(wechat_id,wechat_name,access_token,begin_date,end_date):
    postdata={
            'begin_date': begin_date, 
            'end_date'  : end_date
            }
    postdata = json.dumps(postdata)#转成str格式,不然会报格式不对的错
    upstreammsghoururl='https://api.weixin.qq.com/datacube/getupstreammsghour?access_token=%s'%access_token
    upstreammsghoururl_response=session.post(upstreammsghoururl,data=postdata)
    upstreammsghoururl_json=upstreammsghoururl_response.text
    upstreammsghoururl_json=json.loads(upstreammsghoururl_json)
    upstreammsghoururl_list=upstreammsghoururl_json['list']
    for i in range(0,len(upstreammsghoururl_list)):
        ref_date=upstreammsghoururl_list[i]['ref_date']
        ref_hour=upstreammsghoururl_list[i]['ref_hour']
        msg_type=upstreammsghoururl_list[i]['msg_type']
        msg_user=upstreammsghoururl_list[i]['msg_user']
        msg_count=upstreammsghoururl_list[i]['msg_count']
        upstreammsghour_value=[wechat_id,wechat_name,ref_date,ref_hour,msg_type,msg_user,msg_count]
        cur.execute('insert into upstreammsghour_data values (%s,%s,%s,%s,%s,%s,%s)',upstreammsghour_value)
    conn.commit() 

def get_upstreammsgdist_data(wechat_id,wechat_name,access_token,begin_date,end_date):
    postdata={
            'begin_date': begin_date, 
            'end_date'  : end_date
            }
    postdata = json.dumps(postdata)#转成str格式,不然会报格式不对的错
    upstreammsgdisturl='https://api.weixin.qq.com/datacube/getupstreammsgdist?access_token=%s'%access_token
    upstreammsgdist_response=session.post(upstreammsgdisturl,data=postdata)
    upstreammsgdist_json=upstreammsgdist_response.text
    upstreammsgdist_json=json.loads(upstreammsgdist_json)
    upstreammsgdist_list=upstreammsgdist_json['list']
    for i in range(0,len(upstreammsgdist_list)):
        ref_date=upstreammsgdist_list[i]['ref_date']
        count_interval=upstreammsgdist_list[i]['count_interval']
        msg_user=upstreammsgdist_list[i]['msg_user']
        upstreammsgdist_value=[wechat_id,wechat_name,ref_date,count_interval,msg_user]
        cur.execute('insert into upstreammsgdist_data values (%s,%s,%s,%s,%s)',upstreammsgdist_value)
    conn.commit()      
    
if __name__=='__main__':
    account_data=get_account_data('account_data')
    for index in range(0,len(account_data)):
        wechat_id=account_data[index][0]
        wechat_name=account_data[index][1]
        appid=account_data[index][2]
        secretkey=account_data[index][3]
        access_token=get_access_token(appid,secretkey)
        for day in range(1,2):
            begin_date=set_query_date(day)
            end_date=set_query_date(day)
            try:
                print 'wechat:%s start to get data,index is %s'%(wechat_name,index)
                get_usersummary_data(wechat_id,wechat_name,access_token,begin_date,end_date)
                get_usercumulate_data(wechat_id,wechat_name,access_token,begin_date,end_date)
                get_articlesummary_data(wechat_id,wechat_name,access_token,begin_date,end_date)     
                get_articletotal_data('articletotal_data',wechat_id,wechat_name,access_token,begin_date,end_date)
                get_upstreammsg_data(wechat_id,wechat_name,access_token,begin_date,end_date)
                get_upstreammsghour_data(wechat_id,wechat_name,access_token,begin_date,end_date)
                get_upstreammsgdist_data(wechat_id,wechat_name,access_token,begin_date,end_date)
                print 'wechat:%s get data finished'%wechat_name
                time.sleep(1)
            except Exception,e:
                with open('log.txt','a+') as f:
                    message='%s wechat:%s get data failed,index is %s'%(begin_date,wechat_name.encode('utf-8'),index)
                    f.write('\n')
                    f.write(message)
                    f.close()
    cur.close()
    conn.close()    
