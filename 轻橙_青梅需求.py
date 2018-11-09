# -*- coding: utf-8 -*-
"""
Created on Fri Mar 23 10:01:48 2018

@author: Sun
"""

import os
import smtplib
from email.header import Header
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import datetime
import pandas as pd
import pymysql
import time
from python_cgtools import utils_sql
import numpy as np


class UgcUser6000(object):
    name = "ugc_user_6000"
    host = "116.62.24.58"
    port = 6000
    user = "ugc_user"
    password = "0921WBg2F2qnYjO892EyE28ZcLDHdW28"
    charset = "UTF8"


def get_conn_cur_by_class(_class, _try_times=5):
    while _try_times > 0:
        try:
            conn = pymysql.connect(host=_class.host, port=_class.port, user=_class.user,
                                   password=_class.password, charset=_class.charset)
            cur = conn.cursor()
            cur.execute("SET NAMES 'utf8mb4'")
            cur.execute("SET CHARACTER SET utf8mb4")
            cur.execute("SET CHARACTER_SET_RESULTS=utf8mb4")
            cur.execute("SET CHARACTER_SET_CONNECTION=utf8mb4")
            return conn, cur
        except Exception as e:
            print(e)
            time.sleep(1)
            _try_times -= 1
            if _try_times > 0:
                print("最后第{try_times}次尝试".format(try_times=_try_times))
    assert _try_times != 0, "获取数据库连接失败"


def select_tag(_str):
    _str = str(_str)
    if (_str.find('明星') == -1) & (_str.find('光影') == -1) & (_str.find('真人') == -1):
        return "T"
    else:
        return "F"


db3406, cur3406 = utils_sql.get_conn_cur_by_class(utils_sql.AnalysisUser3406, _try_times=5)

cur3406.execute('''SELECT gindex FROM dbbh_website.org_auto_promo_game  ''')
res = cur3406.fetchall()
gindex = pd.DataFrame(list(res), columns=['gindex'])
gindex_need = list(gindex['gindex'])
# 作品名称、作品编号、作者UID、作者昵称、人气、鲜花数量、收藏、发布日期、最后更新日期、作品字数、是否完结、完结时间
db3406, cur3406 = utils_sql.get_conn_cur_by_class(utils_sql.AnalysisUser3406, _try_times=5)
cur3406.execute('''SELECT
                    	a.gindex,
                    	a.gname,
                    	a.author_uid,
                    	a.play_times,
                    	a.flower,
                       a.fv_times,
                    	FROM_UNIXTIME(a.create_time, '%Y%m%d'),
                    	FROM_UNIXTIME(a.f_release_time, '%Y%m%d'),
                    	complete_flag,
                    	complete_date,
                    	release_word_sum,
                       flower_unlock
                    FROM
                	      dbbh_website.org_game_summary a
                    WHERE
                    	a.gindex IN {0} and check_level >= 2 '''.format(tuple(gindex_need)))
res = cur3406.fetchall()
ginfo1 = pd.DataFrame(list(res),
                      columns=['gindex', '作品名称', '作者UID', '人气', '鲜花数量', '收藏', '发布日期', '最后更新日期', '是否完结', '完结时间', '作品字数',
                               '鲜花锁'])
ginfo1.to_csv('E:\\a发邮件\\中间结果\\ginfo1.csv', sep=',', index=False)

# 作品等级
db3406, cur3406 = utils_sql.get_conn_cur_by_class(utils_sql.AnalysisUser3406, _try_times=5)
cur3406.execute('''SELECT
                    	gindex,
                    	`level`
                    FROM
                    	dbbh_website.org_auto_promo_game                  
                    WHERE
                    	gindex IN {0} '''.format(tuple(gindex_need)))
res = cur3406.fetchall()
df_level = pd.DataFrame(list(res), columns=['gindex', '等级'])
df_level.to_csv('E:\\a发邮件\\中间结果\\df_level.csv', sep=',', index=False)

# 作者组别======================================
db3406, cur3406 = utils_sql.get_conn_cur_by_class(utils_sql.AnalysisUser3406, _try_times=5)
cur3406.execute('''SELECT
                	a.author_uid,
                	b.group_name
                FROM
                	dbbh_website.org_author_tid_group AS a
                JOIN dbbh_website.org_author_tid_group_map AS b ON a.group_id = b.group_id
                AND a.tid = b.tid
                WHERE
                	author_uid IN  {0}'''.format(tuple(list(ginfo1['作者UID']))))
res = cur3406.fetchall()
df_authorgroup = pd.DataFrame(list(res), columns=['作者UID', '作者组别'])
df_authorgroup.to_csv('E:\\a发邮件\\中间结果\\df_authorgroup.csv', sep=',', index=False)

# 标签=========================================
cur3406.execute('''SELECT
                       a.gindex,
                    	group_concat(
                    		b.tname
                    		ORDER BY
                    			b.tname DESC SEPARATOR ';'
                    	) AS tag
                    FROM
                    	dbbh_website.org_tag_game AS a,
                    	dbbh_website.org_tag_summary AS b
                    WHERE
                    	a.tid = b.tid
                    AND a.gindex in {0}
                    group by a.gindex'''.format(tuple(gindex_need)))
res = cur3406.fetchall()
df_tag_tag = pd.DataFrame(list(res), columns=['gindex', '标签'])
df_tag_tag.to_csv('E:\\a发邮件\\中间结果\\df_tag_tag.csv', sep=',', index=False)


# df_tag.to_csv('C:\\Users\\Sun\\Desktop\\game.csv',sep = ',', index = False)
##鲜花收入
# df_tag = pd.DataFrame(columns=['gindex','鲜花收入'])
# for i in  range(len(gindex_need)):
#    i
#    cur6000.execute('''SELECT
#                    	gindex,
#                    	SUM(
#                    		rmb_flower_count * 0.92 + apple_flower_count * 0.64 + basket_flower_count * 0.184 + (
#                    			CASE
#                    			WHEN stat_dt >= 20170101
#                    			AND stat_dt <= 20170131
#                    			AND id >= 36265660
#                    			AND id <= 40529957 THEN
#                    				rainbow_flower_count / 100
#                    			ELSE
#                    				rainbow_flower_count
#                    			END
#                    		) * 0.92 + vote_rmb_flower_count * 0.92 + vote_basket_flower_count * 0.184 + vote_rainbow_flower_count * 0.92 + ios_give_flower_count * 0 + ios_market_flower_count * 0 + ios_basket_flower_count * 0.136 + IFNULL(vote_apple_flower_count, 0) * 0.64 + IFNULL(
#                    			vote_give_apple_flower_count,
#                    			0
#                    		) * 0 + IFNULL(
#                    			vote_marker_apple_flower_count,
#                    			0
#                    		) * 0 + IFNULL(
#                    			vote_basket_apple_flower_count,
#                    			0
#                    		) * 0.136
#                    	) AS flower
#                    FROM
#                    	(
#                    		SELECT
#                    			*
#                    		FROM
#                    			db2017.user_game_stat
#                    		WHERE
#                    			gindex = {0}
#                    		AND (
#                    			rmb_flower_count > 0
#                    			OR apple_flower_count > 0
#                    			OR basket_flower_count > 0
#                    			OR rainbow_flower_count > 0
#                    			OR vote_rmb_flower_count > 0
#                    			OR vote_basket_flower_count > 0
#                    			OR vote_rainbow_flower_count > 0
#                    			OR ios_give_flower_count > 0
#                    			OR ios_market_flower_count > 0
#                    			OR ios_basket_flower_count > 0
#                    			OR IFNULL(vote_apple_flower_count, 0) > 0
#                    			OR IFNULL(
#                    				vote_give_apple_flower_count,
#                    				0
#                    			) > 0
#                    			OR IFNULL(
#                    				vote_marker_apple_flower_count,
#                    				0
#                    			) > 0
#                    			OR IFNULL(
#                    				vote_basket_apple_flower_count,
#                    				0
#                    			) > 0
#                    		)
#                    		UNION
#                    			SELECT
#                    				*
#                    			FROM
#                    				db2016.user_game_stat
#                    			WHERE
#                    				gindex = {0}
#                    			AND (
#                    				rmb_flower_count > 0
#                    				OR apple_flower_count > 0
#                    				OR basket_flower_count > 0
#                    				OR rainbow_flower_count > 0
#                    				OR vote_rmb_flower_count > 0
#                    				OR vote_basket_flower_count > 0
#                    				OR vote_rainbow_flower_count > 0
#                    				OR ios_give_flower_count > 0
#                    				OR ios_market_flower_count > 0
#                    				OR ios_basket_flower_count > 0
#                    				OR IFNULL(vote_apple_flower_count, 0) > 0
#                    				OR IFNULL(
#                    					vote_give_apple_flower_count,
#                    					0
#                    				) > 0
#                    				OR IFNULL(
#                    					vote_marker_apple_flower_count,
#                    					0
#                    				) > 0
#                    				OR IFNULL(
#                    					vote_basket_apple_flower_count,
#                    					0
#                    				) > 0
#                    			)
#                    	) AS a
#                    GROUP BY
#                    	gindex'''.format( gindex_need[i]))
#    res = cur6000.fetchall()
#    df = pd.DataFrame(list(res),columns = ['gindex','鲜花收入'])
#    df_tag = pd.concat([df_tag, df])    


# 作品更新字数===============================================================


def save(filename, contents):
    fh = open(filename, 'a', encoding='utf-8')
    fh.write(contents)
    fh.close()


###有更新行为的游戏

# 上一个月的第一天
a = int((datetime.datetime.today() - datetime.timedelta(days=time.localtime().tm_wday + 7)).strftime("%Y%m%d"))
# 上一个月的最后一天
b = int((datetime.datetime.today() - datetime.timedelta(days=time.localtime().tm_wday + 1)).strftime("%Y%m%d"))
db3406, cur3406 = utils_sql.get_conn_cur_by_class(utils_sql.UgcUserOn3406, _try_times=5)

cur3406.execute('''select distinct(gindex) from dbbh_website.org_game_version 
                where pub_day between {0} and {1} AND pub_mode = 1 '''.format(a, b))
res = cur3406.fetchall()
game = list(i[0] for i in res)

update_con = []
for i in range(len(game)):
    print(i)
    db3406, cur3406 = utils_sql.get_conn_cur_by_class(utils_sql.UgcUserOn3406, _try_times=5)
    cur3406.execute('''select gindex,word_sum,pub_day from dbbh_website.org_game_version 
                    where gindex = %s AND pub_mode = 1 ''' % game[i])
    res = cur3406.fetchall()
    df = pd.DataFrame(list(res), columns=['作品id', '总字数', '发布日期'])
    index_number = df[(df['发布日期'] >= a) & (df['发布日期'] <= b)].index.tolist()
    if index_number[0] > 0:
        df_new = df[index_number[0] - 1:index_number[-1] + 1]
        word_list = list(df_new['总字数'])
        chazhi = list(map(lambda x: x[0] - x[1], zip(word_list[1:len(word_list)], word_list[0:len(word_list) - 1])))
        num_above0 = [i for i in chazhi if i > 0]
        if len(num_above0) == 0:
            pass
        else:
            update_con.append((game[i], sum(num_above0)))
    else:
        df_new = df[index_number[0]:index_number[-1] + 1]
        word_list = list(df_new['总字数'])
        word_list.insert(0, 0)
        chazhi = list(map(lambda x: x[0] - x[1], zip(word_list[1:len(word_list)], word_list[0:len(word_list) - 1])))
        num_above0 = [i for i in chazhi if i > 0]
        if len(num_above0) == 0:
            pass
        else:
            update_con.append((game[i], sum(num_above0)))

df_update = pd.DataFrame(update_con, columns=['gindex', '本周更新字数'])
df_update.to_csv('E:\\a发邮件\\中间结果\\df_update.csv', sep=',', index=False)

a = int((datetime.datetime.today() - datetime.timedelta(days=time.localtime().tm_wday + 30)).strftime("%Y%m%d"))

b = int((datetime.datetime.today() - datetime.timedelta(days=time.localtime().tm_wday + 1)).strftime("%Y%m%d"))

db3406, cur3406 = utils_sql.get_conn_cur_by_class(utils_sql.UgcUserOn3406, _try_times=5)

cur3406.execute('''select distinct(gindex) from dbbh_website.org_game_version 
                where pub_day between {0} and {1} AND pub_mode = 1 '''.format(a, b))
res = cur3406.fetchall()
game = list(i[0] for i in res)
len(game)
update_con = []
for i in range(len(game)):
    print(i)
    db3406, cur3406 = utils_sql.get_conn_cur_by_class(utils_sql.UgcUserOn3406, _try_times=5)
    cur3406.execute('''select gindex,word_sum,pub_day from dbbh_website.org_game_version 
                    where gindex = %s AND pub_mode = 1 ''' % game[i])
    res = cur3406.fetchall()
    df = pd.DataFrame(list(res), columns=['作品id', '总字数', '发布日期'])
    index_number = df[(df['发布日期'] >= a) & (df['发布日期'] <= b)].index.tolist()
    if index_number[0] > 0:
        df_new = df[index_number[0] - 1:index_number[-1] + 1]
        word_list = list(df_new['总字数'])
        chazhi = list(map(lambda x: x[0] - x[1], zip(word_list[1:len(word_list)], word_list[0:len(word_list) - 1])))
        num_above0 = [i for i in chazhi if i > 0]
        if len(num_above0) == 0:
            pass
        else:
            update_con.append((game[i], sum(num_above0)))
    else:
        df_new = df[index_number[0]:index_number[-1] + 1]
        word_list = list(df_new['总字数'])
        word_list.insert(0, 0)
        chazhi = list(map(lambda x: x[0] - x[1], zip(word_list[1:len(word_list)], word_list[0:len(word_list) - 1])))
        num_above0 = [i for i in chazhi if i > 0]
        if len(num_above0) == 0:
            pass
        else:
            update_con.append((game[i], sum(num_above0)))

df_update_month = pd.DataFrame(update_con, columns=['gindex', '本月更新字数'])
df_update_month.to_csv('E:\\a发邮件\\中间结果\\df_update_month.csv', sep=',', index=False)

# 经典时间
db3406, cur3406 = utils_sql.get_conn_cur_by_class(utils_sql.AnalysisUser3406, _try_times=5)

cur3406.execute('''SELECT
                    	a.gindex,
                    	FROM_UNIXTIME(score, '%Y%m%d')
                    FROM
                    	dbbh_website.org_tag_game AS a,
                    	dbbh_website.org_tag_summary AS b
                    WHERE
                    	a.tid = b.tid
                    AND b.tname LIKE '%经典%'
                    AND gindex IN {0}
                    GROUP BY a.gindex '''.format(tuple(gindex_need)))
res = cur3406.fetchall()
df_jingdian = pd.DataFrame(list(res), columns=['gindex', '经典时间'])
df_jingdian.to_csv('E:\\a发邮件\\中间结果\\df_jingdian.csv', sep=',', index=False)

# 编推时间
db3406, cur3406 = utils_sql.get_conn_cur_by_class(utils_sql.AnalysisUser3406, _try_times=5)

cur3406.execute('''SELECT
                    	a.gindex,
                    	FROM_UNIXTIME(score, '%Y%m%d')
                    FROM
                    	dbbh_website.org_tag_game AS a,
                    	dbbh_website.org_tag_summary AS b
                    WHERE
                    	a.tid = b.tid
                    AND b.tname LIKE '%编推%'
                    AND gindex IN {0}
                    GROUP BY a.gindex '''.format(tuple(gindex_need)))
res = cur3406.fetchall()
df_biantui = pd.DataFrame(list(res), columns=['gindex', '编推时间'])
df_biantui.to_csv('E:\\a发邮件\\中间结果\\df_biantui.csv', sep=',', index=False)

# 是否使用新标签
db3406, cur3406 = utils_sql.get_conn_cur_by_class(utils_sql.UgcUserOn3406, _try_times=5)
sql = """  
      select  gindex,'是' as tag from dbbh_website.org_game_tag_collect
          """
cur3406.execute(sql)
res = cur3406.fetchall()
df_newtag = pd.DataFrame(list(res), columns=['gindex', '是否使用新标签'])
df_newtag.to_csv('E:\\a发邮件\\中间结果\\df_newtag.csv', sep=',', index=False)

# 是否使用新标签
db3508, cur3508 = utils_sql.get_conn_cur_by_class(utils_sql.UgcUserOn3508, _try_times=5)
sql = """  
      SELECT uid,(CASE WHEN contract_type =0 THEN '旧合同'  ELSE '新合同' END ) FROM  dbbh_admin.finance_legal_bonus_grade 
          """
cur3508.execute(sql)
res = cur3508.fetchall()
df_hetong = pd.DataFrame(list(res), columns=['作者UID', '合同类别'])
df_hetong.to_csv('E:\\a发邮件\\中间结果\\df_hetong.csv', sep=',', index=False)

# 作者昵称
nick_name = pd.DataFrame(columns=['作者UID', '作者昵称'])
for i in range(0, 100):
    print(i)
    db3402, cur3402 = utils_sql.get_conn_cur_by_class(utils_sql.UgcUserOn3402, _try_times=5)
    sql = """SELECT uid,nick_name FROM dbbh_passport.uc_user_info_{0} WHERE uid in {1} """.format(i, tuple(
        list(ginfo1['作者UID'])))
    cur3402.execute(sql)
    res = cur3402.fetchall()
    nick_name_data = pd.DataFrame(list(res), columns=['作者UID', '作者昵称'])
    nick_name = pd.concat([nick_name, nick_name_data], ignore_index=True)
nick_name.to_csv('E:\\a发邮件\\中间结果\\nick_name.csv', sep=',', index=False)

# 编辑最后备注
db3508, cur3508 = utils_sql.get_conn_cur_by_class(utils_sql.UgcUserOn3508, _try_times=5)
sql = """  
              SELECT
        	a.gindex,
        	a.editor_remark
        FROM
        	dbbh_admin.admin_editor_postil AS a
        JOIN (
        	SELECT
        		gindex,
        		MAX(pos_time) AS v
        	FROM
        		dbbh_admin.admin_editor_postil
        	WHERE
        		gindex IN {0}
        	GROUP BY
        		gindex
        ) AS b ON a.gindex = b.gindex
        AND a.pos_time = b.v
        GROUP BY a.gindex
          """.format(tuple(gindex_need))
cur3508.execute(sql)
res = cur3508.fetchall()
df_postil = pd.DataFrame(list(res), columns=['gindex', '编辑最后备注'])
df_postil.to_csv('E:\\a发邮件\\中间结果\\df_postil.csv', sep=',', index=False)


###====================鲜花============
def datelist(beginDate, endDate):
    date_l = [datetime.datetime.strftime(x, '%Y%m%d') for x in list(pd.date_range(start=beginDate, end=endDate))]
    return date_l


da = str((datetime.datetime.today() - datetime.timedelta(days=time.localtime().tm_wday + 1)).strftime("%Y%m%d"))
db = str((datetime.datetime.today() - datetime.timedelta(days=time.localtime().tm_wday + 7)).strftime("%Y%m%d"))
dc = str((datetime.datetime.today() - datetime.timedelta(days=time.localtime().tm_wday + 30)).strftime("%Y%m%d"))

day = datelist(datetime.datetime(int(db[0:4]), int(db[4:6]), int(db[6:8])),
               datetime.datetime(int(da[0:4]), int(da[4:6]), int(da[6:8])))

# 查鲜花
db6000, cur6000 = utils_sql.get_conn_cur_by_class(utils_sql.UgcUser6000, _try_times=5)
if int(db[0:6]) != int(da[0:6]):
    sql = """SELECT gindex,SUM(flower) FROM (SELECT gindex,SUM(CASE WHEN coin_type=3 THEN coin_num/100 ELSE coin_num END) AS flower FROM org_user_give_log.`org_log_user_coin_{3}` 
            WHERE coin_type IN (3,4,5,6,7,8,10,11) AND TYPE IN (50,52,144,137,138,139) AND create_date >={1} and create_date <={2} AND gindex IN {0}  GROUP BY gindex UNION all 
            SELECT gindex,SUM(CASE WHEN coin_type=3 THEN coin_num/100 ELSE coin_num END) AS flower FROM org_user_give_log.`org_log_user_coin_{4}` 
            WHERE coin_type IN (3,4,5,6,7,8,10,11) AND TYPE IN (50,52,144,137,138,139) AND create_date >={1} and create_date <={2} AND gindex IN {0}  GROUP BY gindex  ) as a
            GROUP BY gindex """.format(tuple(gindex_need), day[0], day[-1], int(db[0:6]), int(da[0:6]))
else:
    sql = """SELECT gindex,SUM(flower) FROM (SELECT gindex,SUM(CASE WHEN coin_type=3 THEN coin_num/100 ELSE coin_num END) AS flower FROM org_user_give_log.`org_log_user_coin_{3}` 
        WHERE coin_type IN (3,4,5,6,7,8,10,11) AND TYPE IN (50,52,144,137,138,139) AND create_date >={1} and create_date <={2} AND gindex IN {0}  GROUP BY gindex  ) as a
        GROUP BY gindex """.format(tuple(gindex_need), day[0], day[-1], int(db[0:6]), int(da[0:6]))

cur6000.execute(sql)
res = cur6000.fetchall()
flower_num_week = pd.DataFrame(list(res), columns=['gindex', '当周鲜花增长数'])
flower_num_week.to_csv('E:\\a发邮件\\中间结果\\flower_num_week.csv', sep=',', index=False)

day = datelist(datetime.datetime(int(dc[0:4]), int(dc[4:6]), int(dc[6:8])),
               datetime.datetime(int(da[0:4]), int(da[4:6]), int(da[6:8])))
db6000, cur6000 = utils_sql.get_conn_cur_by_class(utils_sql.UgcUser6000, _try_times=5)
if int(da[0:6]) != int(dc[0:6]):
    sql = """SELECT gindex,SUM(flower) FROM (SELECT gindex,SUM(CASE WHEN coin_type=3 THEN coin_num/100 ELSE coin_num END) AS flower FROM org_user_give_log.`org_log_user_coin_{3}` 
            WHERE coin_type IN (3,4,5,6,7,8,10,11) AND TYPE IN (50,52,144,137,138,139) AND create_date >={1} and create_date <={2} AND gindex IN {0}  GROUP BY gindex UNION all 
            SELECT gindex,SUM(CASE WHEN coin_type=3 THEN coin_num/100 ELSE coin_num END) AS flower FROM org_user_give_log.`org_log_user_coin_{4}` 
            WHERE coin_type IN (3,4,5,6,7,8,10,11) AND TYPE IN (50,52,144,137,138,139) AND create_date >={1} and create_date <={2} AND gindex IN {0}  GROUP BY gindex  ) as a
            GROUP BY gindex """.format(tuple(gindex_need), day[0], day[-1], int(dc[0:6]), int(da[0:6]))
else:
    sql = """SELECT gindex,SUM(flower) FROM (SELECT gindex,SUM(CASE WHEN coin_type=3 THEN coin_num/100 ELSE coin_num END) AS flower FROM org_user_give_log.`org_log_user_coin_{3}` 
        WHERE coin_type IN (3,4,5,6,7,8,10,11) AND TYPE IN (50,52,144,137,138,139) AND create_date >={1} and create_date <={2} AND gindex IN {0}  GROUP BY gindex  ) as a
        GROUP BY gindex """.format(tuple(gindex_need), day[0], day[-1], int(dc[0:6]), int(da[0:6]))
cur6000.execute(sql)
res = cur6000.fetchall()
flower_num_month = pd.DataFrame(list(res), columns=['gindex', '当月鲜花增长数'])
flower_num_month.to_csv('E:\\a发邮件\\中间结果\\flower_num_month.csv', sep=',', index=False)
# ============付费转化率=======================
# 每个游戏的总游玩人数（截止到统计当天的前一天）
user_playuser = pd.DataFrame(columns=['gindex', '总游玩人数'])
for i in range(0, 100):
    print(i)
    db3407, cur3407 = utils_sql.get_conn_cur_by_class(utils_sql.UgcUserOn3407, _try_times=5)
    sql = """ 
        SELECT gindex,COUNT(DISTINCT uid) FROM platform_count_01.user_game_record_{0}  where gindex in {1} GROUP BY gindex
        """.format(i, tuple(gindex_need))
    cur3407.execute(sql)
    res = cur3407.fetchall()
    res = pd.DataFrame(list(res), columns=['gindex', '总游玩人数'])
    user_playuser = pd.concat([user_playuser, res], ignore_index=True)
user_playuser = user_playuser.groupby(['gindex'], as_index=False).agg({'总游玩人数': np.sum})
user_playuser.to_csv('E:\\a发邮件\\中间结果\\user_playuser.csv', sep=',', index=False)

# 每个游戏的总付费人数
db3406, cur3406 = utils_sql.get_conn_cur_by_class(utils_sql.AnalysisUser3406, _try_times=5)
cur3406.execute('''select gindex ,SUM(num) from (
                    select gindex,COUNT(DISTINCT uid) as num from dbbh_website.org_game_earn_from_user_0
                    where coin_type=2 and gindex IN {0} group by gindex union ALL
                    select gindex,COUNT(DISTINCT uid) as num from dbbh_website.org_game_earn_from_user_1
                    where coin_type=2 and gindex IN {0} group by gindex union ALL
                    select  gindex,COUNT(DISTINCT uid) as num from dbbh_website.org_game_earn_from_user_2
                    where coin_type=2 and gindex IN {0} group by gindex union ALL
                    select  gindex,COUNT(DISTINCT uid) as num from dbbh_website.org_game_earn_from_user_3
                    where coin_type=2 and gindex IN {0} group by gindex union ALL
                    select  gindex,COUNT(DISTINCT uid) as num from dbbh_website.org_game_earn_from_user_4
                    where coin_type=2 and gindex IN {0} group by gindex) as t 
                    GROUP BY gindex '''.format(tuple(gindex_need)))
res = cur3406.fetchall()
user_payuser = pd.DataFrame(list(res), columns=['gindex', '总献花人数'])
user_payuser.to_csv('E:\\a发邮件\\中间结果\\user_payuser.csv', sep=',', index=False)

# 总付费转化率
user_all_rate = pd.merge(user_playuser, user_payuser, on='gindex', how='left')
user_all_rate = user_all_rate.fillna(0)
user_all_rate[['总游玩人数']] = user_all_rate[['总游玩人数']].astype(int)
user_all_rate[['总献花人数']] = user_all_rate[['总献花人数']].astype(int)
user_all_rate['总付费转化率'] = user_all_rate['总献花人数'] / user_all_rate['总游玩人数']
user_all_rate = user_all_rate[['gindex', '总付费转化率']]
user_all_rate.to_csv('E:\\a发邮件\\中间结果\\user_all_rate.csv', sep=',', index=False)

# 每个游戏的最近一月游玩人数
day_list = datelist(datetime.datetime(int(dc[0:4]), int(dc[4:6]), int(dc[6:8])),
                    datetime.datetime(int(da[0:4]), int(da[4:6]), int(da[6:8])))

user_playuser = pd.DataFrame(columns=['gindex', 'uid'])
for i in day_list:
    print(i)
    db3407, cur3407 = utils_sql.get_conn_cur_by_class(utils_sql.UgcUserOn3407, _try_times=5)
    sql = """ 
        SELECT gindex,uid FROM platform_count_01.org_game_daily_by_user_{0} WHERE gindex IN {1}
        """.format(i, tuple(gindex_need))
    cur3407.execute(sql)
    res = cur3407.fetchall()
    res = pd.DataFrame(list(res), columns=['gindex', 'uid'])
    user_playuser = pd.concat([user_playuser, res], ignore_index=True)

user_playuser_month = user_playuser.groupby(['gindex'], as_index=False).agg({'uid': pd.Series.nunique})
user_playuser_month.columns = ['gindex', '当月游玩人数']
user_playuser_month.to_csv('E:\\a发邮件\\中间结果\\user_playuser_month.csv', sep=',', index=False)

# 每个游戏的最近一月付费人数
db6000, cur6000 = utils_sql.get_conn_cur_by_class(utils_sql.UgcUser6000, _try_times=5)
sql = """SELECT gindex,COUNT(DISTINCT uid) FROM (SELECT gindex, uid FROM org_user_give_log.`org_log_user_coin_{3}` 
        WHERE coin_type IN (3,4,5,6,7,8,10,11) AND TYPE IN (50,52,144,137,138,139) AND create_date >={1} and create_date <={2} AND gindex IN {0}  GROUP BY gindex, uid UNION all
        SELECT gindex, uid FROM org_user_give_log.`org_log_user_coin_{4}` 
        WHERE coin_type IN (3,4,5,6,7,8,10,11) AND TYPE IN (50,52,144,137,138,139) AND create_date >={1} and create_date <={2} AND gindex IN {0}  GROUP BY gindex, uid  ) as a
        GROUP BY gindex """.format(tuple(gindex_need), day[0], day[-1], int(dc[0:6]), int(da[0:6]))
cur6000.execute(sql)
res = cur6000.fetchall()
user_payuser_month = pd.DataFrame(list(res), columns=['gindex', '当月献花人数'])
user_payuser_month.to_csv('E:\\a发邮件\\中间结果\\user_payuser_month.csv', sep=',', index=False)

# 当月付费转化率
user_rate = pd.merge(user_playuser_month, user_payuser_month, on='gindex', how='left')
user_rate = user_rate.fillna(0)
user_rate[['当月游玩人数']] = user_rate[['当月游玩人数']].astype(int)
user_rate[['当月献花人数']] = user_rate[['当月献花人数']].astype(int)
user_rate['当月付费转化率'] = user_rate['当月献花人数'] / user_rate['当月游玩人数']
user_rate = user_rate[['gindex', '当月付费转化率']]
user_rate.to_csv('E:\\a发邮件\\中间结果\\user_rate.csv', sep=',', index=False)


# 人气

def datelist(beginDate, endDate):
    date_l = [datetime.datetime.strftime(x, '%Y%m%d') for x in list(pd.date_range(start=beginDate, end=endDate))]
    return date_l


da = str((datetime.datetime.today() - datetime.timedelta(days=time.localtime().tm_wday + 1)).strftime("%Y%m%d"))
db = str((datetime.datetime.today() - datetime.timedelta(days=time.localtime().tm_wday + 7)).strftime("%Y%m%d"))
dc = str((datetime.datetime.today() - datetime.timedelta(days=time.localtime().tm_wday + 30)).strftime("%Y%m%d"))

db6066, cur6066 = utils_sql.get_conn_cur_by_class(utils_sql.UgcUserOn3406, _try_times=5)
sql = """
    SELECT gindex,guid FROM  dbbh_website.org_game_summary   WHERE check_level >= 2
     """
cur6066.execute(sql)
res = cur6066.fetchall()
res_g = pd.DataFrame(list(res), columns=['gindex', 'guid'])
# gindex= list(res['gindex'])
guid = list(res_g['guid'])
day = datelist(datetime.datetime(int(dc[0:4]), int(dc[4:6]), int(dc[6:8])),
               datetime.datetime(int(da[0:4]), int(da[4:6]), int(da[6:8])))
db3505, cur3505 = utils_sql.get_conn_cur_by_class(utils_sql.UgcUserOn3505, _try_times=5)
if int(dc[0:6]) != int(da[0:6]):
    sql = """SELECT
            	guid,
            	SUM(num)
            FROM
            	(
            		SELECT
            			guid,
            			SUM(var4 + var6 + var7 + var8) AS num
            		FROM
            			game_record.orange_log_vars_{3}
            		WHERE
            			`day` >= {1}
            		AND `day` < {2}
            		AND guid IN {0}
            		GROUP BY
            			guid
            		UNION ALL
            			SELECT
            				guid,
            				SUM(var4 + var6 + var7 + var8) AS num
            			FROM
            				game_record.orange_log_vars_{4}
            			WHERE
            				`day` >= {1}
            			AND `day` <= {2}
            			AND guid IN {0}
            			GROUP BY
            				guid
            	) a
            GROUP BY
            	guid""".format(tuple(guid), day[0], day[-1], int(dc[0:6]), int(da[0:6]))
else:
    sql = """SELECT
        	guid,
        	SUM(num)
        FROM
        	(
        		SELECT
        			guid,
        			SUM(var4 + var6 + var7 + var8) AS num
        		FROM
        			game_record.orange_log_vars_{3}
        		WHERE
        			`day` >= {1}
        		AND `day` < {2}
        		AND guid IN {0}
        		GROUP BY
        			guid
        	) a
        GROUP BY
        	guid""".format(tuple(guid), day[0], day[-1], int(dc[0:6]), int(da[0:6]))
cur3505.execute(sql)
res = cur3505.fetchall()
play_num_month = pd.DataFrame(list(res), columns=['guid', '当月人气'])
play_num_month = pd.merge(res_g, play_num_month, on='guid', how='left')
play_num_month = play_num_month[["gindex", "当月人气"]]
play_num_month.to_csv('E:\\a发邮件\\中间结果\\play_num_month.csv', sep=',', index=False)

day = datelist(datetime.datetime(int(db[0:4]), int(db[4:6]), int(db[6:8])),
               datetime.datetime(int(da[0:4]), int(da[4:6]), int(da[6:8])))
db3505, cur3505 = utils_sql.get_conn_cur_by_class(utils_sql.UgcUserOn3505, _try_times=5)
if int(db[0:6]) != int(da[0:6]):
    sql = """SELECT
            	guid,
            	SUM(num)
            FROM
            	(
            		SELECT
            			guid,
            			SUM(var4 + var6 + var7 + var8) AS num
            		FROM
            			game_record.orange_log_vars_{3}
            		WHERE
            			`day` >= {1}
            		AND `day` < {2}
            		AND guid IN {0}
            		GROUP BY
            			guid
            		UNION ALL
            			SELECT
            				guid,
            				SUM(var4 + var6 + var7 + var8) AS num
            			FROM
            				game_record.orange_log_vars_{4}
            			WHERE
            				`day` >= {1}
            			AND `day` <= {2}
            			AND guid IN {0}
            			GROUP BY
            				guid
            	) a
            GROUP BY
            	guid""".format(tuple(guid), day[0], day[-1], int(db[0:6]), int(da[0:6]))
else:
    sql = """SELECT
        	guid,
        	SUM(num)
        FROM
        	(
        		SELECT
        			guid,
        			SUM(var4 + var6 + var7 + var8) AS num
        		FROM
        			game_record.orange_log_vars_{3}
        		WHERE
        			`day` >= {1}
        		AND `day` < {2}
        		AND guid IN {0}
        		GROUP BY
        			guid
        	) a
        GROUP BY
        	guid""".format(tuple(guid), day[0], day[-1], int(db[0:6]), int(da[0:6]))

cur3505.execute(sql)
res = cur3505.fetchall()
play_num_week = pd.DataFrame(list(res), columns=['guid', '当周人气'])
play_num_week = pd.merge(res_g, play_num_week, on='guid', how='left')
play_num_week = play_num_week[["gindex", "当周人气"]]
play_num_week.to_csv('E:\\a发邮件\\中间结果\\play_num_week.csv', sep=',', index=False)


###====================更新频率============
def datelist(beginDate, endDate):
    date_l = [datetime.datetime.strftime(x, '%Y%m%d') for x in list(pd.date_range(start=beginDate, end=endDate))]
    return date_l


da = str((datetime.datetime.today() - datetime.timedelta(days=time.localtime().tm_wday + 1)).strftime("%Y%m%d"))
db = str((datetime.datetime.today() - datetime.timedelta(days=time.localtime().tm_wday + 7)).strftime("%Y%m%d"))
dc = str((datetime.datetime.today() - datetime.timedelta(days=time.localtime().tm_wday + 30)).strftime("%Y%m%d"))

day = datelist(datetime.datetime(int(db[0:4]), int(db[4:6]), int(db[6:8])),
               datetime.datetime(int(da[0:4]), int(da[4:6]), int(da[6:8])))
db3406, cur3406 = utils_sql.get_conn_cur_by_class(utils_sql.UgcUserOn3406, _try_times=5)
cur3406.execute('''select gindex,COUNT(*) from dbbh_website.org_game_version 
                where pub_day >={0} and pub_day <={1} AND pub_mode = 1  AND gindex in {2} GROUP BY gindex '''.format(
    day[0], day[-1], tuple(gindex_need)))
res = cur3406.fetchall()
pub_num_week = pd.DataFrame(list(res), columns=['gindex', '当周更新次数'])
pub_num_week.to_csv('E:\\a发邮件\\中间结果\\pub_num_week.csv', sep=',', index=False)

day = datelist(datetime.datetime(int(dc[0:4]), int(dc[4:6]), int(dc[6:8])),
               datetime.datetime(int(da[0:4]), int(da[4:6]), int(da[6:8])))
db3406, cur3406 = utils_sql.get_conn_cur_by_class(utils_sql.UgcUserOn3406, _try_times=5)
cur3406.execute('''select gindex,COUNT(*) from dbbh_website.org_game_version 
                where pub_day >={0} and pub_day <={1} AND pub_mode = 1  AND gindex in {2} GROUP BY gindex '''.format(
    day[0], day[-1], tuple(gindex_need)))
res = cur3406.fetchall()
pub_num_month = pd.DataFrame(list(res), columns=['gindex', '当月更新次数'])
pub_num_month.to_csv('E:\\a发邮件\\中间结果\\pub_num_month.csv', sep=',', index=False)

db3406, cur3406 = utils_sql.get_conn_cur_by_class(utils_sql.UgcUserOn3406, _try_times=5)
cur3406.execute('''select gindex,COUNT(*) from dbbh_website.org_game_version 
                where  pub_day <={0} AND pub_mode = 1  AND gindex in {1} GROUP BY gindex '''.format(day[-1],
                                                                                                    tuple(gindex_need)))
res = cur3406.fetchall()
pub_num_all = pd.DataFrame(list(res), columns=['gindex', '总更新次数'])
pub_num_all.to_csv('E:\\a发邮件\\中间结果\\pub_num_all.csv', sep=',', index=False)

# ginfo1 = pd.read_csv('E:\\a发邮件\\中间结果\\ginfo1.csv', sep=',',header = 0,encoding = 'GBK')
# df_level = pd.read_csv('E:\\a发邮件\\中间结果\\df_level.csv', sep=',',header = 0,encoding = 'GBK')
# nick_name = pd.read_csv('E:\\a发邮件\\中间结果\\nick_name.csv', sep=',',header = 0,encoding = 'GBK')
# df_update = pd.read_csv('E:\\a发邮件\\中间结果\\df_update.csv', sep=',',header = 0,encoding = 'GBK')
# df_update_month = pd.read_csv('E:\\a发邮件\\中间结果\\df_update_month.csv', sep=',',header = 0,encoding = 'GBK')
# df_authorgroup = pd.read_csv('E:\\a发邮件\\中间结果\\df_authorgroup.csv', sep=',',header = 0,encoding = 'GBK')
# df_hetong = pd.read_csv('E:\\a发邮件\\中间结果\\df_hetong.csv', sep=',',header = 0,encoding = 'GBK')
# df_tag_tag = pd.read_csv('E:\\a发邮件\\中间结果\\df_tag_tag.csv', sep=',',header = 0,encoding = 'GBK')
# df_jingdian = pd.read_csv('E:\\a发邮件\\中间结果\\df_jingdian.csv', sep=',',header = 0,encoding = 'GBK')
# df_biantui = pd.read_csv('E:\\a发邮件\\中间结果\\df_biantui.csv', sep=',',header = 0,encoding = 'GBK')
# df_newtag = pd.read_csv('E:\\a发邮件\\中间结果\\df_newtag.csv', sep=',',header = 0,encoding = 'GBK')
# df_postil = pd.read_csv('E:\\a发邮件\\中间结果\\df_postil.csv', sep=',',header = 0,encoding = 'GBK')
# flower_num_week = pd.read_csv('E:\\a发邮件\\中间结果\\flower_num_week.csv', sep=',',header = 0,encoding = 'GBK')
# flower_num_month = pd.read_csv('E:\\a发邮件\\中间结果\\flower_num_month.csv', sep=',',header = 0,encoding = 'GBK')
# user_all_rate = pd.read_csv('E:\\a发邮件\\中间结果\\user_all_rate.csv', sep=',',header = 0,encoding = 'GBK')
# user_rate = pd.read_csv('E:\\a发邮件\\中间结果\\user_rate.csv', sep=',',header = 0,encoding = 'GBK')
# play_num_month = pd.read_csv('E:\\a发邮件\\中间结果\\play_num_month.csv', sep=',',header = 0,encoding = 'GBK')
# play_num_week = pd.read_csv('E:\\a发邮件\\中间结果\\play_num_week.csv', sep=',',header = 0,encoding = 'GBK')
# pub_num_month = pd.read_csv('E:\\a发邮件\\中间结果\\pub_num_month.csv', sep=',',header = 0,encoding = 'GBK')
# pub_num_week = pd.read_csv('E:\\a发邮件\\中间结果\\pub_num_week.csv', sep=',',header = 0,encoding = 'GBK')
# pub_num_all = pd.read_csv('E:\\a发邮件\\中间结果\\pub_num_all.csv', sep=',',header = 0,encoding = 'GBK')


data_res = pd.merge(ginfo1, df_level, on='gindex', how='left')
data_res = pd.merge(data_res, nick_name, on='作者UID', how='left')
data_res = pd.merge(data_res, df_update, on='gindex', how='left')
data_res = pd.merge(data_res, df_update_month, on='gindex', how='left')
data_res = pd.merge(data_res, df_authorgroup, on='作者UID', how='left')
data_res = pd.merge(data_res, df_hetong, on='作者UID', how='left')
data_res = pd.merge(data_res, df_tag_tag, on='gindex', how='left')
data_res = pd.merge(data_res, df_jingdian, on='gindex', how='left')
data_res = pd.merge(data_res, df_biantui, on='gindex', how='left')
data_res = pd.merge(data_res, df_newtag, on='gindex', how='left')
data_res = pd.merge(data_res, df_postil, on='gindex', how='left')
data_res = pd.merge(data_res, flower_num_week, on='gindex', how='left')
data_res = pd.merge(data_res, flower_num_month, on='gindex', how='left')
data_res = pd.merge(data_res, user_all_rate, on='gindex', how='left')
data_res = pd.merge(data_res, user_rate, on='gindex', how='left')
data_res = pd.merge(data_res, play_num_month, on='gindex', how='left')
data_res = pd.merge(data_res, play_num_week, on='gindex', how='left')
data_res = pd.merge(data_res, pub_num_month, on='gindex', how='left')
data_res = pd.merge(data_res, pub_num_week, on='gindex', how='left')
data_res = pd.merge(data_res, pub_num_all, on='gindex', how='left')
data_res["平均周更字数"] = data_res["本月更新字数"] / 4
data_final = data_res[['gindex', '作品名称', '作者UID', '作者昵称', '等级', '标签', '鲜花锁',
                       '鲜花数量', '当周鲜花增长数', '当月鲜花增长数', '总付费转化率', '当月付费转化率',
                       '人气', '当周人气', '当月人气',
                       '作品字数', '本周更新字数', '平均周更字数',
                       '当周更新次数', '当月更新次数', '总更新次数',
                       '发布日期', '最后更新日期', '是否完结', '完结时间', '经典时间', '编推时间', '作者组别', '编辑最后备注', '合同类别', '收藏']]
# data_final = data_res[['gindex', '作品名称', '作者UID', '作者昵称', '等级', '标签', '鲜花锁',
#                       '鲜花数量','当周鲜花增长数', '当月鲜花增长数', '总付费转化率', '当月付费转化率', 
#                       '人气', '当周人气', '当月人气',
#                       '作品字数',
#                       '当周更新次数', '当月更新次数', '总更新次数',
#                       '发布日期', '最后更新日期', '是否完结','完结时间','经典时间', '编推时间', '作者组别','编辑最后备注','合同类别']]


# flower_num_month.to_csv('C:\\Users\\Sun\\Desktop\\d.csv',sep = ',', index = False)

# data_res.to_csv('C:\\Users\\Sun\\Desktop\\人气.csv',sep = ',', index = False)
# df_update.to_csv('C:\\Users\\Sun\\Desktop\\df_update.csv',sep = ',', index = False)
# data_res = pd.merge(ginfo1,df_authorgroup,on = '作者UID',how = 'left')

# data_flower = pd.read_csv('C:\\Users\\Sun\\Desktop\\20180323flowerback.csv', sep=',',header = 0,encoding = 'GBK')
# data_flower = data_flower[["gindex","当周鲜花增长数","当月鲜花增长数","总付费转化率","当月付费转化率"]]
# data_res = pd.merge(data_res,data_flower,on = 'gindex',how = 'left')
# 区分大区
# data_res['k'] = data_res['标签'].map(select_tag)
# data_res1 = data_res[data_res['k'] =='T' ]
# data_res1 = data_res1.drop(['k'],axis=1)
# data_res2 = data_res[data_res['k'] =='F' ]
# data_res2 = data_res2.drop(['k'],axis=1)
# 存成excel
# writer = pd.ExcelWriter('E:\\a发邮件\\qingmeixuqiu.xlsx')
# data_final.to_excel(writer,sheet_name='内容部数据需求', index = False)
# data_res2.to_excel(writer,sheet_name='明星光影真人', index = False)
# writer.save()
date = time.strftime("%Y%m%d")
data_final.to_csv('E:\\a发邮件\\qingmeixuqiu_{0}.csv'.format(date), sep=',', index=False)


# data_res.to_csv('C:\\Users\\Sun\\Desktop\\20180323_final.csv',sep = ',', index = False)

# ==============================================================================
# #上传到ftp
# ==============================================================================
# from ftplib import FTP
#
# ftp = FTP()
# ftp.connect("118.178.122.222",3271)
# ftp.login('poa','CwAwCirhyLiEnWTC')
#    
##从本地上传文件到ftp
# def uploadfile(remotepath, localpath):
#    bufsize = 3271
#    fp = open(localpath, 'rb')
#    ftp.storbinary('STOR ' + remotepath, fp, bufsize)
#    ftp.set_debuglevel(0)
#    fp.close()
#
# uploadfile('/poa/gz/work_{}.csv'.format(time.strftime("%Y%m%d")),'C:\\Users\\Sun\\Desktop\\青梅需求_{0}.csv'.format(time.strftime("%Y%m%d")))


def get_res(_database, _sql, _columns):
    db, cur = utils_sql.get_conn_cur_by_class(_database, _try_times=5)
    cur.execute(_sql)
    res = cur.fetchall()
    data = pd.DataFrame(list(res), columns=_columns)
    return data


# 轻橙总工程数


# DBA监控统一邮箱
class Email(object):
    name = "橙光data"
    smtpserver = "smtp.exmail.qq.com"
    address = "jisi@66rpg.com"
    password = "Abcdzyx2"


class Email_to(object):
    address1 = "2880843213@qq.com"
    address2 = "3001906487@qq.com"
    address3 = "yingtao@66rpg.com"
    address4 = "3003292336@qq.com"
    address5 = "yemao@66rpg.com"


# 补全邮箱地址
def add_email_username(_email, _seperate="@"):
    if _seperate in _email:
        return _email.split(_seperate)[0] + '<' + _email + ">"
    else:
        return '<' + _email + ">"


# 发邮件
def send_email(_subject, _receivers, _mail_body, _attachment_path=None, _attachment_name=None, _class=Email):
    if isinstance(_receivers, str):
        _receivers = [_receivers]
    # 邮件实例
    message = MIMEMultipart()
    message["From"] = Header(_class.name + "<" + _class.address + ">", charset="utf-8")
    # 群发邮件显示收件人时会有轻微的格式错乱，尚未找到合适的办法，但不影响发送
    # message["To"] = Header(", ".join(map(add_email_username, _receivers)), charset="utf-8")
    message["To"] = Header(", ".join(_receivers), charset="utf-8")
    message["Subject"] = Header(_subject, charset="utf-8")

    # 邮件正文内容
    message.attach(MIMEText(_text=_mail_body, _subtype="plain", _charset="utf-8"))

    # 增加附件
    if _attachment_path is not None:
        if not os.path.exists(_attachment_path):
            raise IOError("文件路径不存在，请重试...")
        if not os.path.isfile(_attachment_path):
            raise IOError("必须传入文件路径而非文件夹路径，请重试...")
        if _attachment_name is None or len(_attachment_name) == 0:
            _attachment_name = os.path.split(_attachment_path)[1]
            # 构造附件，传送当前目录下的 test.txt 文件
        with open(_attachment_path, "rb") as fp:
            content = fp.read()
        attachment = MIMEText(_text=content, _subtype="base64", _charset="utf-8")
        attachment["Content-Type"] = "application/octet-stream"
        if _attachment_name is not None and len(_attachment_name) != 0:
            attachment["Content-Disposition"] = "attachment; filename=%s" % _attachment_name
        else:
            attachment["Content-Disposition"] = "attachment; filename=%s" % _attachment_name
        message.attach(attachment)

    # 发送邮件，如果有异常则向上抛出
    smtp = None  # 如果不事先声明，加入第一句就报错，那么finally中的smtp就是没有预先定义的
    try:
        smtp = smtplib.SMTP()
        smtp.connect(_class.smtpserver)
        smtp.login(user=_class.address, password=_class.password)
        smtp.sendmail(from_addr=_class.address, to_addrs=_receivers, msg=message.as_string())
    except smtplib.SMTPException as e:
        print(type(e))
        print(e)
        print("Error: 无法发送邮件")
        raise e
    finally:
        if smtp is not None:
            smtp.quit()


# tt
# send_email("上周情况", [Email_to.address1, Email_to.address2,Email_to.address3,Email_to.address4,Email_to.address5], "请看附件", _attachment_path = 'E:\\a发邮件\\qingmeixuqiu.csv',_attachment_name=os.path.basename('E:\\a发邮件\\qingmeixuqiu.csv'))

send_email("上周情况_{0}".format(date),
           [Email_to.address1, Email_to.address2, Email_to.address3, Email_to.address4, Email_to.address5], "请查看附件",
           _attachment_path='E:\\a发邮件\\qingmeixuqiu_{0}.csv'.format(date),
           _attachment_name=os.path.basename('E:\\a发邮件\\qingmeixuqiu_{0}.csv'.format(date)))
