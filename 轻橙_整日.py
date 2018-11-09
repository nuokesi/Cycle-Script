# -*- coding: utf-8 -*-
"""
Created on Mon May 14 16:40:37 2018

@author: Sun
"""

import numpy as np
import pandas as pd

from cg_jisi.utils_mail import mail_send
from cg_jisi.utils_time import relative_date
from cg_jisi.utils_sql import df_from_sql, get_res
from cg_jisi.utils_sql import UgcUserOn3406, UgcUserOn3510, UgcUserOn3407, UgcUserOn3408, UgcUserOn3513


def get_3510(_date):
    # 新建轻橙工程数量
    tt_sum = 0
    for i in range(0, 100):
        print('3510', i)
        sql = f"""SELECT count(uid)  from sae_project.orange_uid_guid_map_{i} where source=2 and DATE_FORMAT(create_time,"%Y%m%d")= {_date}"""
        tt_sum += df_from_sql(UgcUserOn3510, sql, [''])[''][0]
    dt['新建轻橙工程数量'] = tt_sum


def get_gindex(_date, database=UgcUserOn3406):
    # 带轻橙标签的游戏编号
    sql = f"""SELECT gindex, guid, check_level
             FROM dbbh_website.org_game_summary
             WHERE gindex IN (SELECT gindex
            		            FROM dbbh_website.org_tag_game
            		            WHERE tid = 12337)
             AND FROM_UNIXTIME(create_time, "%Y%m%d") <={_date}
             """
    gindex = df_from_sql(database, sql, ['gindex', 'guid', 'check_level'])

    # 带轻橙标签的游戏编号过审作品
    gindex_check = gindex[gindex['check_level'] >= 2][['gindex', 'guid']]
    return gindex, gindex_check


def get_3406(date, gindex, gindex_check, database=UgcUserOn3406):

    sql = f"""SELECT count(DISTINCT(gindex))
             FROM dbbh_website.org_game_version 
             WHERE channel_id=2 
             AND version=1
             AND pub_day = {date} 
             GROUP BY pub_day"""
    dt['轻橙新发布的作品数量'] = get_res(database, sql)[0][0]

    sql = f"""SELECT count(DISTINCT(gindex))
             FROM dbbh_website.org_game_version
             WHERE channel_id = 2
             AND pub_mode > 0 
             AND pub_day = {date}
             AND gindex not in(SELECT DISTINCT(gindex)
                                FROM dbbh_website.org_game_version
                                WHERE channel_id = 2
                                AND pub_mode > 0 
                                AND pub_day < {date})"""
    dt['正式发布过的新增轻橙作品数量'] = get_res(database, sql)[0][0]

    sql = f"""SELECT count(DISTINCT(gindex))
             FROM dbbh_website.org_game_version 
             WHERE channel_id=2
             AND word_sum > 0  
             AND pub_mode > 0
             AND pub_day = {date}
             AND gindex not in(SELECT DISTINCT(gindex)
                               FROM dbbh_website.org_game_version 
                               WHERE channel_id=2
                               AND word_sum > 0  
                               AND pub_mode > 0
                               AND pub_day < {date})"""
    dt['正式发布过字数大于0的新增轻橙作品数量'] = get_res(database, sql)[0][0]

    # 当日过审轻橙作品
    sql = f"""SELECT COUNT(*)
             FROM dbbh_website.org_game_summary
             WHERE gindex IN {tuple(gindex['gindex'])}
             AND FROM_UNIXTIME(passed_time, "%Y%m%d") = {date}"""
    dt['新过审轻橙作品数量'] = get_res(database, sql)[0][0]

    # 各等级作品分布
    sql = f"""SELECT COUNT(gindex) from dbbh_website.org_auto_promo_game WHERE gindex IN {tuple(gindex_check['gindex'])} GROUP BY LEVEL"""
    res = get_res(database, sql)
    dt['L1作品数'] = res[0][0]   
    dt['L2作品数'] = res[1][0]
    dt['L3作品数'] = res[2][0]
    dt['L4作品数'] = res[3][0]
    dt['L5作品数'] = res[4][0]

    # 当日更新字数
    sql = f'''select distinct(gindex) 
             from dbbh_website.org_game_version 
             where pub_day = {date} 
             and gindex in {tuple(gindex['gindex'])} 
             AND pub_mode = 1'''
    game = list(df_from_sql(database, sql, [''])[''])

    update_con = []
    for i in range(len(game)):
        print(i)
        sql = f'''select gindex,word_sum,pub_day from dbbh_website.org_game_version
                        where gindex = {game[i]} AND pub_mode = 1 '''
        df = df_from_sql(database, sql, ['作品id', '总字数', '发布日期'])
        index_number = df[df['发布日期'] == date].index.tolist()
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

    data_word = pd.DataFrame(update_con, columns=['gindex', '当日更新字数'])
    dt['当日更新字数'] = data_word["当日更新字数"].sum()

    sql = f"""SELECT sum(num) from (select count(a.cid) as num from dbbh_website.org_game_comment_summary_0 a, dbbh_website.org_game_comment_detail_0 b where a.cid = b.cid and a.gindex in {tuple(gindex['gindex'])} and a.add_day= {date} union all 
            select count(a.cid) as num from dbbh_website.org_game_comment_summary_1 a, dbbh_website.org_game_comment_detail_1 b where a.cid = b.cid and a.gindex in {tuple(gindex['gindex'])} and a.add_day= {date}  union all 
            select count(a.cid) as num from dbbh_website.org_game_comment_summary_2 a, dbbh_website.org_game_comment_detail_2 b where a.cid = b.cid and a.gindex in {tuple(gindex['gindex'])} and a.add_day= {date}  union all 
            select count(a.cid) as num from dbbh_website.org_game_comment_summary_3 a, dbbh_website.org_game_comment_detail_3 b where a.cid = b.cid and a.gindex in {tuple(gindex['gindex'])} and a.add_day= {date}  union all 
            select count(a.cid) as num from dbbh_website.org_game_comment_summary_4 a, dbbh_website.org_game_comment_detail_4 b where a.cid = b.cid and a.gindex in {tuple(gindex['gindex'])} and a.add_day= {date}  union all 
            select count(a.cid) as num from dbbh_website.org_game_comment_summary_5 a, dbbh_website.org_game_comment_detail_5 b where a.cid = b.cid and a.gindex in {tuple(gindex['gindex'])} and a.add_day= {date}  union all 
            select count(a.cid) as num from dbbh_website.org_game_comment_summary_6 a, dbbh_website.org_game_comment_detail_6 b where a.cid = b.cid and a.gindex in {tuple(gindex['gindex'])} and a.add_day= {date}  union all 
            select count(a.cid) as num from dbbh_website.org_game_comment_summary_7 a, dbbh_website.org_game_comment_detail_7 b where a.cid = b.cid and a.gindex in {tuple(gindex['gindex'])} and a.add_day= {date}  union all 
            select count(a.cid) as num from dbbh_website.org_game_comment_summary_8 a, dbbh_website.org_game_comment_detail_8 b where a.cid = b.cid and a.gindex in {tuple(gindex['gindex'])} and a.add_day= {date}  union all 
            select count(a.cid) as num from dbbh_website.org_game_comment_summary_9 a, dbbh_website.org_game_comment_detail_9 b where a.cid = b.cid and a.gindex in {tuple(gindex['gindex'])} and a.add_day= {date}) as a """
    dt['评论'] = get_res(database, sql)[0][0]

    # 轻橙完结作品总数
    sql = """SELECT COUNT(*)
            FROM dbbh_website.org_game_summary
            WHERE gindex IN (SELECT gindex
                             FROM dbbh_website.org_game_summary
                             WHERE gindex IN (SELECT gindex
                                            	 FROM dbbh_website.org_tag_game
                                            	 WHERE tid = 12337)
                             AND FROM_UNIXTIME(create_time, "%Y%m%d") <={0})
            AND complete_date <= {0} and complete_date >0""".format(date)
    dt['轻橙完结作品总数量'] = get_res(database, sql)[0][0]


def get_author_count(date):
    # 作者人数
    sql = f"""SELECT  COUNT(DISTINCT author_uid) 
             FROM  dbbh_website.org_game_summary 
             WHERE gindex in (SELECT gindex
                              FROM dbbh_website.org_game_summary
                              WHERE gindex IN (SELECT gindex
                        		FROM dbbh_website.org_tag_game
                        		WHERE tid = 12337)
            AND FROM_UNIXTIME(create_time, "%Y%m%d") <={date}) 
            AND gindex in (SELECT gindex 
                           from dbbh_website.org_game_version 
                           WHERE pub_mode = 1 AND  gindex in (SELECT gindex
                                                              FROM dbbh_website.org_game_summary
                                                              WHERE gindex IN (SELECT gindex
                                                        		  FROM dbbh_website.org_tag_game
                                                        		  WHERE tid = 12337)
                           AND FROM_UNIXTIME(create_time, "%Y%m%d") <={date}))"""
    res1 = get_res(UgcUserOn3406, sql)[0][0]
    
    sql = """ SELECT uid FROM website_userinfo.org_user_sign WHERE `status` = 2 """
    res = get_res(UgcUserOn3408, sql)
    
    uid = [i[0] for i in res]

    sql = f"""SELECT COUNT(DISTINCT author_uid) 
             FROM   dbbh_website.org_game_summary 
             WHERE  gindex in (SELECT gindex
                               FROM   dbbh_website.org_game_summary
                               WHERE  gindex IN (SELECT gindex
                        		 FROM   dbbh_website.org_tag_game
                        		 WHERE  tid = 12337
                        	   )
            AND FROM_UNIXTIME(create_time, "%Y%m%d") <={date})
            AND gindex in (SELECT gindex 
                           from   dbbh_website.org_game_version 
                           WHERE  pub_mode = 1 
                           AND    gindex in (SELECT gindex 
                                             FROM   dbbh_website.org_game_summary
                                             WHERE  gindex IN (SELECT gindex
                                                        		   FROM   dbbh_website.org_tag_game
                                                        		   WHERE  tid = 12337
                                                        	     )
                           AND FROM_UNIXTIME(create_time, "%Y%m%d") <={date}) 
                          )
            AND author_uid in {tuple(uid)}"""
    res2 = get_res(UgcUserOn3406, sql)[0][0]
    dt['正式发布轻橙作品签约作者总数'] = res2
    dt['正式发布轻橙作品非签约作者总数'] = res1 - res2
    return uid


def get_3408(date, gindex, database=UgcUserOn3408):
    # 鲜花
    sql = """ SELECT SUM(CASE WHEN coin_type=3 THEN coin_num/100 ELSE coin_num END) AS flower FROM website_userinfo.org_log_user_coin_{0} 
            WHERE coin_type IN (3,4,5,6,7,8,10,11) 
            AND TYPE IN (50,52,144,137,138,139) AND create_date ={1}  AND gindex IN {2}  """.format(str(date)[0:6], date, tuple(gindex['gindex']))
    dt['当日鲜花总数'] = int(get_res(database, sql)[0][0])

    sql = f"""SELECT SUM(num) FROM (SELECT gindex ,COUNT(*) as num  FROM website_userinfo.org_user_fav_game_0 WHERE gindex IN {tuple(gindex['gindex'])} and add_day = {date}  group by gindex UNION ALL
            SELECT gindex ,COUNT(*) as num FROM website_userinfo.org_user_fav_game_1 WHERE gindex IN  {tuple(gindex['gindex'])} and add_day = {date}  group by gindex UNION ALL
            SELECT gindex ,COUNT(*) as num FROM website_userinfo.org_user_fav_game_2 WHERE gindex IN  {tuple(gindex['gindex'])} and add_day = {date}  group by gindex UNION ALL
            SELECT gindex ,COUNT(*) as num FROM website_userinfo.org_user_fav_game_3 WHERE gindex IN  {tuple(gindex['gindex'])} and add_day = {date}  group by gindex UNION ALL
            SELECT gindex ,COUNT(*) as num FROM website_userinfo.org_user_fav_game_4 WHERE gindex IN  {tuple(gindex['gindex'])} and add_day = {date}  group by gindex) as a"""
    dt['收藏'] = get_res(database, sql)[0][0]


def get_3513(date, gindex, database=UgcUserOn3513):
    sql = """SELECT SUM(share_num) as s from user_act.share_user_share_game_{0}
             WHERE share_date  = {1} AND gindex IN {2}  
             """.format(str(date)[0:6], date, tuple(gindex['gindex']))
    dt['分享'] = get_res(database, sql)[0][0]

    sql = """SELECT SUM(num) FROM ( select count(gindex) as num from game_outer_attr.org_game_star_record_0 where gindex in {0} AND FROM_UNIXTIME(add_time, "%Y%m%d")={1}    UNION ALL 
            select count(gindex) as num  from game_outer_attr.org_game_star_record_1 where gindex in {0} AND FROM_UNIXTIME(add_time, "%Y%m%d")={1}   UNION ALL 
            select count(gindex) as num from game_outer_attr.org_game_star_record_2 where gindex in {0} AND FROM_UNIXTIME(add_time, "%Y%m%d")={1}   UNION ALL 
            select count(gindex) as num from game_outer_attr.org_game_star_record_3 where gindex in {0} AND FROM_UNIXTIME(add_time, "%Y%m%d")={1}  UNION ALL 
            select count(gindex) as num from game_outer_attr.org_game_star_record_4 where gindex in {0} AND FROM_UNIXTIME(add_time, "%Y%m%d")={1}   UNION ALL 
            select count(gindex) as num from game_outer_attr.org_game_star_record_5 where gindex in {0} AND FROM_UNIXTIME(add_time, "%Y%m%d")={1}   UNION ALL 
            select count(gindex) as num from game_outer_attr.org_game_star_record_6 where gindex in {0} AND FROM_UNIXTIME(add_time, "%Y%m%d")={1}   UNION ALL 
            select count(gindex) as num from game_outer_attr.org_game_star_record_7 where gindex in {0} AND FROM_UNIXTIME(add_time, "%Y%m%d")={1}   UNION ALL 
            select count(gindex) as num from game_outer_attr.org_game_star_record_8 where gindex in {0} AND FROM_UNIXTIME(add_time, "%Y%m%d")={1}   UNION ALL 
            select count(gindex) as num from game_outer_attr.org_game_star_record_9 where gindex in {0} AND FROM_UNIXTIME(add_time, "%Y%m%d")={1}   UNION ALL 
            select count(gindex) as num from game_outer_attr.org_game_star_record_10 where gindex in {0} AND FROM_UNIXTIME(add_time, "%Y%m%d")={1}   UNION ALL 
            select count(gindex) as num from game_outer_attr.org_game_star_record_11 where gindex in {0} AND FROM_UNIXTIME(add_time, "%Y%m%d")={1}   UNION ALL 
            select count(gindex) as num from game_outer_attr.org_game_star_record_12 where gindex in {0} AND FROM_UNIXTIME(add_time, "%Y%m%d")={1}   UNION ALL 
            select count(gindex) as num from game_outer_attr.org_game_star_record_13 where gindex in {0} AND FROM_UNIXTIME(add_time, "%Y%m%d")={1}   UNION ALL 
            select count(gindex) as num from game_outer_attr.org_game_star_record_14 where gindex in {0} AND FROM_UNIXTIME(add_time, "%Y%m%d")={1}  ) as a
            """.format(tuple(gindex['gindex']), date)
    dt['点赞'] = get_res(database, sql)[0][0]


def get_3407(date, gindex, database=UgcUserOn3407):
    # 轻橙用户数(玩过轻橙作品的用户数)
    user_playuser = []
    for i in range(0, 100):
        print('轻橙用户数', i)
        sql = """SELECT gindex,COUNT(DISTINCT uid) FROM platform_count_01.user_game_record_{0}  where gindex in {1} GROUP BY gindex
              """.format(i, tuple(gindex['gindex']))
        user_playuser.append(df_from_sql(database, sql, ['gindex', '总游玩人数']))
    user_playuser = pd.concat(user_playuser, ignore_index=True)
    user_playuser = user_playuser.groupby(['gindex'], as_index=False).agg({'总游玩人数': np.sum})
    dt['总轻橙用户数'] = user_playuser["总游玩人数"].sum()

    # 轻橙用户数(玩过轻橙作品的用户数全去重)
    user_playuser = []
    tup = tuple(gindex['gindex'])
    for i in range(0, 100):
        print('轻橙用户数去重', i)
        sql = """ SELECT COUNT(DISTINCT uid) FROM platform_count_01.user_game_record_{0}  where gindex in {1}""".format(i, tup)
        user_playuser.append(df_from_sql(database, sql, ['总游玩人数']))
    user_playuser = pd.concat(user_playuser, ignore_index=True)
    dt['总轻橙用户数（全去重）'] = user_playuser["总游玩人数"].sum()

    # 当日游玩人数和总时长
    sql = """ 
        SELECT COUNT(DISTINCT uid),sum(run_time)/3600 FROM platform_count_01.org_game_daily_by_user_{0}  where gindex in {1}
        """.format(date, tuple(gindex['gindex']))
    res = get_res(database, sql)
    dt['每日去重用户数'] = res[0][0]
    dt['每日用户总游玩时长（小时）'] = int(res[0][1])


def get_qingcheng(d):
    get_3510(d)
    gindex, gindex_check = get_gindex(d)
    get_3406(d, gindex, gindex_check)
    get_3408(d, gindex)
    get_3513(d, gindex)
    get_3407(d, gindex)
    uid = get_author_count(d)

    # 新发布工程的签约作者人数
    tt_sum1 = []
    for i in range(0, 100):
        print('签约作者数', i)
        sql = f"""SELECT uid, gguid  from sae_project.orange_uid_guid_map_{i} where source=2 and DATE_FORMAT(create_time,"%Y%m%d")<= {d}"""
        tt_sum1.append(df_from_sql(UgcUserOn3510, sql, ['作者UID', 'guid']))
    tt_sum1 = pd.concat(tt_sum1, ignore_index=True)
    project_uid = list(set(list(tt_sum1['作者UID'])))
    project_uid_sign = [v for v in project_uid if v in uid]
    dt['新建轻橙工程签约作者总数'] = len(project_uid_sign)
    dt['新建轻橙工程非签约作者总数'] = len(project_uid) - len(project_uid_sign)

    data_all = pd.DataFrame([dt])
    data_all = data_all[['日期', '新建轻橙工程数量', '轻橙新发布的作品数量', '正式发布过的新增轻橙作品数量',
                         '正式发布过字数大于0的新增轻橙作品数量', '新过审轻橙作品数量', 'L1作品数', 'L2作品数',
                         'L3作品数', 'L4作品数', 'L5作品数', '轻橙完结作品总数量', '当日更新字数',
                         '当日鲜花总数', '分享', '收藏', '点赞', '评论', '正式发布轻橙作品签约作者总数',
                         '正式发布轻橙作品非签约作者总数', '新建轻橙工程签约作者总数', '新建轻橙工程非签约作者总数',
                         '总轻橙用户数', '总轻橙用户数（全去重）', '每日去重用户数', '每日用户总游玩时长（小时）']]
    data_all.to_excel(f'P:/a/轻橙/月报/{d}_轻橙.xlsx', index=False)
    data_all = pd.DataFrame.stack(data_all, level=-1, dropna=True)
    return data_all


if __name__ == '__main__':
    dt = {}
    script_date = int(relative_date(-1).replace('-', ''))
    dt['日期'] = script_date
    data_alln = get_qingcheng(script_date)
    mail_send(f"新增轻橙工程数量_整日{script_date}", ["xiyou@66rpg.com"], f"{data_alln}",
              f"P:/a/轻橙/月报/{script_date}_轻橙.xlsx", "1.xlsx")
