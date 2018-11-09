# -*- coding: utf-8 -*-
"""
Created on Fri Sep 21 11:36:21 2018

@author: Sun 祭司修改版
"""


import os
import zipfile
import pandas as pd

import cg_jisi.utils_sql as js
import cg_jisi.utils_time as jt
import cg_jisi.utils_mail as jm


class Email_to(object):
    address1 = "xiyou@66rpg.com"  # 芸豆
    address2 = "3001906487@qq.com"


def get_gindex(_date, _tag):
    # 带轻橙标签的游戏编号
    database = js.AnalysisUser3406
    sql = """SELECT gindex, guid
             FROM dbbh_website.org_game_summary
             WHERE gindex IN 
             ( 
             SELECT gindex
             FROM dbbh_website.org_tag_game
             WHERE tid = {1}
              )
            AND FROM_UNIXTIME(passed_time, "%Y%m%d") <={0} and check_level >=2 """.format(_date, _tag)
    return js.df_from_sql(database, sql, ['gindex', 'guid'])


def get_list():
    database = js.UgcUserOn3406
    sql = """SELECT tid, tname
            FROM dbbh_website.org_tag_summary 
            WHERE tname like '#%#'
            AND status in (0, 9)
            """
    df_list = js.df_from_sql(database, sql, ['tag', 'tname'])
    return list(df_list['tag']), list(df_list['tname'])


def get_qingcheng(_date, _tag, _tname):
    gindex = get_gindex(_date, _tag)
    data_count = pd.DataFrame({'日期': ['{0}'.format(_date)], '收录作品总数量': [len(gindex)]}, columns=['日期', '收录作品总数量'])
    if len(gindex) == 0:
        data_all = pd.DataFrame({'日期': ['{0}'.format(_date)], '标签': ['{0}_截止统计日期无过审作品'.format(_tname)]},
                                columns=['日期', '标签'])
        return data_all
    else:
        # 当日过审轻橙作品
        sql = """SELECT {0} AS p, COUNT(*)
                 FROM dbbh_website.org_game_summary
                 WHERE gindex IN {1}
                 AND FROM_UNIXTIME(passed_time, "%Y%m%d") = {0}""".format(_date, tuple(list(gindex['gindex'])))
        data_pass_gindex = js.df_from_sql(js.UgcUserOn3406, sql, ['日期', '当日新过审作品数量'])

        # 当日更新字数
        sql = '''select distinct(gindex) from dbbh_website.org_game_version 
                        where pub_day = {0} and gindex in {1} AND pub_mode = 1 '''.format(_date,
                                                                                          tuple(list(gindex['gindex'])))
        result = js.df_from_sql(js.UgcUserOn3406, sql, ['gid'])
        game = list(result['gid'])
        update_con = []
        for i in range(len(game)):
            print(i)
            sql = '''select gindex,word_sum,pub_day from dbbh_website.org_game_version 
                            where gindex = %s AND pub_mode = 1 ''' % game[i]
            df = js.df_from_sql(js.UgcUserOn3406, sql, ['作品id', '总字数', '发布日期'])
            index_number = df[df['发布日期'] == _date].index.tolist()
            if index_number[0] > 0:
                df_new = df[index_number[0] - 1:index_number[-1] + 1]
                word_list = list(df_new['总字数'])
                differ = list(
                    map(lambda x: x[0] - x[1], zip(word_list[1:len(word_list)], word_list[0:len(word_list) - 1])))
                num_above0 = [i for i in differ if i > 0]
                if len(num_above0) == 0:
                    pass
                else:
                    update_con.append((game[i], sum(num_above0)))
            else:
                df_new = df[index_number[0]:index_number[-1] + 1]
                word_list = list(df_new['总字数'])
                word_list.insert(0, 0)
                differ = list(
                    map(lambda x: x[0] - x[1], zip(word_list[1:len(word_list)], word_list[0:len(word_list) - 1])))
                num_above0 = [i for i in differ if i > 0]
                if len(num_above0) == 0:
                    pass
                else:
                    update_con.append((game[i], sum(num_above0)))

        df_update = pd.DataFrame(update_con, columns=['gindex', '当日更新字数'])
        data_word = pd.DataFrame({'日期': ['{0}'.format(_date)], '当日更新字数': [df_update["当日更新字数"].sum()]},
                                 columns=['日期', '当日更新字数'])

        # 鲜花、人气、分享、收藏、点赞、评论
        db3408, cur3408 = js.get_conn_cur_by_class(js.UgcUserOn3408, _try_times=5)
        sql = """ SELECT SUM(CASE WHEN coin_type=3 THEN coin_num/100 ELSE coin_num END) AS flower FROM website_userinfo.`org_log_user_coin_{0}` 
                WHERE coin_type IN (3,4,5,6,7,8,10,11) 
                AND TYPE IN (50,52,144,137,138,139) AND create_date ={1}  AND gindex IN {2}  """.format(str(_date)[0:6],
                                                                                                        _date, tuple(
                list(gindex['gindex'])))
        cur3408.execute(sql)
        result = cur3408.fetchall()
        flower_num = pd.DataFrame({'日期': ['{0}'.format(_date)], '当日鲜花总数': [int(result[0][0] or 0)]},
                                  columns=['日期', '当日鲜花总数'])

        db3505, cur3505 = js.get_conn_cur_by_class(js.UgcUserOn3505, _try_times=5)
        sql = """ SELECT SUM(var4 + var6 + var7 + var8) AS '总人气'
                  FROM game_record.orange_log_vars_{0}
                  WHERE guid in {2} and day = {1}
                 """.format(str(_date)[0:6], _date, tuple(list(gindex['guid'])))
        cur3505.execute(sql)
        result = cur3505.fetchall()
        renqi_num = pd.DataFrame({'日期': ['{0}'.format(_date)], '当日人气总数': [int(result[0][0] or 0)]},
                                 columns=['日期', '当日人气总数'])

        db3513, cur3513 = js.get_conn_cur_by_class(js.UgcUserOn3513, _try_times=5)
        sql = """
             SELECT {1} AS p,SUM(share_num) as s from user_act.share_user_share_game_{0}
             WHERE share_date  = {1} AND gindex IN {2}  
             """.format(str(_date)[0:6], _date, tuple(list(gindex['gindex'])))
        cur3513.execute(sql)
        result = cur3513.fetchall()
        share_num = pd.DataFrame(list(result), columns=['日期', '分享'])

        db3408, cur3408 = js.get_conn_cur_by_class(js.UgcUserOn3408, _try_times=5)
        sql = """SELECT {1} AS p,SUM(num) FROM (SELECT gindex ,COUNT(*) as num  FROM website_userinfo.org_user_fav_game_0 WHERE gindex IN {0} and add_day = {1}  group by gindex UNION ALL
                SELECT gindex ,COUNT(*) as num FROM website_userinfo.org_user_fav_game_1 WHERE gindex IN  {0} and add_day = {1}  group by gindex UNION ALL
                SELECT gindex ,COUNT(*) as num FROM website_userinfo.org_user_fav_game_2 WHERE gindex IN  {0} and add_day = {1}  group by gindex UNION ALL
                SELECT gindex ,COUNT(*) as num FROM website_userinfo.org_user_fav_game_3 WHERE gindex IN  {0} and add_day = {1}  group by gindex UNION ALL
                SELECT gindex ,COUNT(*) as num FROM website_userinfo.org_user_fav_game_4 WHERE gindex IN  {0} and add_day = {1}  group by gindex ) as a
                """.format(tuple(list(gindex['gindex'])), _date)
        cur3408.execute(sql)
        result = cur3408.fetchall()
        fav_num = pd.DataFrame(list(result), columns=['日期', '收藏'])

        db6066, cur6066 = js.get_conn_cur_by_class(js.UgcUserOn3513, _try_times=5)
        sql = """SELECT {1} AS p,SUM(num) FROM ( select count(gindex) as num from game_outer_attr.org_game_star_record_0 where gindex in {0} AND FROM_UNIXTIME(add_time, "%Y%m%d")={1}    UNION ALL 
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
                """.format(tuple(list(gindex['gindex'])), _date)
        cur6066.execute(sql)
        result = cur6066.fetchall()
        dianzan_num = pd.DataFrame(list(result), columns=['日期', '点赞'])

        # 作者人数
        db3406, cur3406 = js.get_conn_cur_by_class(js.UgcUserOn3406, _try_times=5)
        sql = """SELECT  COUNT(DISTINCT author_uid) FROM  dbbh_website.org_game_summary WHERE gindex in {0} AND gindex in (SELECT gindex from dbbh_website.org_game_version WHERE pub_mode = 1 AND  gindex in {0} )
                  """.format(tuple(list(gindex['gindex'])))
        cur3406.execute(sql)
        res1 = cur3406.fetchall()
        pd.DataFrame({'日期': ['{0}'.format(_date)], '发布轻橙作品总作者数': [res1[0][0]]},
                     columns=['日期', '发布轻橙作品总作者数'])
        # 签约作者人数
        db3408, cur3408 = js.get_conn_cur_by_class(js.UgcUserOn3408, _try_times=5)
        sql = """ SELECT uid FROM website_userinfo.org_user_sign WHERE `status` = 2 """
        cur3408.execute(sql)
        result = cur3408.fetchall()
        uid = [i[0] for i in result]
        db3406, cur3406 = js.get_conn_cur_by_class(js.UgcUserOn3406, _try_times=5)
        sql = """SELECT  COUNT(DISTINCT author_uid) FROM  dbbh_website.org_game_summary WHERE gindex in {0} AND gindex in (SELECT gindex from dbbh_website.org_game_version WHERE pub_mode = 1 AND  gindex in {0} )
                 AND author_uid in {1}""".format(tuple(list(gindex['gindex'])), tuple(uid))
        cur3406.execute(sql)
        res2 = cur3406.fetchall()
        data_signauthor = pd.DataFrame({'日期': ['{0}'.format(_date)], '正式发布轻橙作品签约作者总数': [res2[0][0]]},
                                       columns=['日期', '正式发布轻橙作品签约作者总数'])
        data_unsignauthor = pd.DataFrame({'日期': ['{0}'.format(_date)], '正式发布轻橙作品非签约作者总数': [res1[0][0] - res2[0][0]]},
                                         columns=['日期', '正式发布轻橙作品非签约作者总数'])
        # 轻橙用户数(玩过轻橙作品的用户数)
        db3407, cur3407 = js.get_conn_cur_by_class(js.UgcUserOn3407, _try_times=5)
        sql = """ 
            SELECT COUNT(DISTINCT uid) FROM platform_count_01.org_game_daily_by_user_{0}  where gindex in {1}
            """.format(_date, tuple(list(gindex['gindex'])))
        cur3407.execute(sql)
        result = cur3407.fetchall()
        data_userall = pd.DataFrame({'日期': ['{0}'.format(_date)], '每日用户数': [result[0][0]]}, columns=['日期', '每日用户数'])

        flower_num[["日期"]] = flower_num[["日期"]].astype(int)
        share_num[["日期"]] = share_num[["日期"]].astype(int)
        fav_num[["日期"]] = fav_num[["日期"]].astype(int)
        dianzan_num[["日期"]] = dianzan_num[["日期"]].astype(int)
        renqi_num[["日期"]] = renqi_num[["日期"]].astype(int)
        data_signauthor[["日期"]] = data_signauthor[["日期"]].astype(int)
        data_unsignauthor[["日期"]] = data_unsignauthor[["日期"]].astype(int)
        data_word[["日期"]] = data_word[["日期"]].astype(int)
        data_userall[["日期"]] = data_userall[["日期"]].astype(int)
        data_count[["日期"]] = data_count[["日期"]].astype(int)

        data_all = pd.merge(data_count, data_pass_gindex, on='日期', how='left')
        data_all = pd.merge(data_all, data_word, on='日期', how='left')
        data_all = pd.merge(data_all, flower_num, on='日期', how='left')
        data_all = pd.merge(data_all, share_num, on='日期', how='left')
        data_all = pd.merge(data_all, fav_num, on='日期', how='left')
        data_all = pd.merge(data_all, dianzan_num, on='日期', how='left')
        data_all = pd.merge(data_all, renqi_num, on='日期', how='left')
        data_all = pd.merge(data_all, data_signauthor, on='日期', how='left')
        data_all = pd.merge(data_all, data_unsignauthor, on='日期', how='left')
        data_all = pd.merge(data_all, data_userall, on='日期', how='left')
        data_all['标签'] = '{0}'.format(_tname)
        data_all.to_csv('P:\\a\\轻橙\\每日数据\\{0}_轻橙_{1}.csv'.format(_date, _tag), sep=',', index=False)
        return data_all


date = int(jt.relative_date(-1))
tag_list, tname_list = get_list()
data_alln = []
for tag, tname in zip(tag_list, tname_list):
    print(tag, tname)
    res = get_qingcheng(date, tag, tname)
    data_alln.append(res)
data_alln = pd.concat(data_alln, ignore_index=True)
data_alln = data_alln[['日期', '收录作品总数量', '当日新过审作品数量', '当日更新字数',
                       '当日鲜花总数', '分享', '收藏', '点赞', '当日人气总数',
                       '正式发布轻橙作品签约作者总数', '正式发布轻橙作品非签约作者总数',
                       '每日用户数', '标签']]

data_alln.to_excel('P:\\a\\轻橙\\每日数据\\活动数据{0}.xlsx'.format(date), index=False)
file = 'P:\\a\\轻橙\\每日数据\\活动数据{0}.xlsx'.format(date)
f = zipfile.ZipFile('P:\\a\\轻橙\\每日数据\\shuju.zip', 'w', zipfile.ZIP_DEFLATED)
f.write(file, os.path.basename(file))
f.close()
jm.mail_send("轻橙活动数据{0}".format(date), [Email_to.address1, Email_to.address2], "详见附件中<活动数据>表格",
             'P:\\a\\轻橙\\每日数据\\活动数据{0}.xlsx'.format(date), '活动数据{0}.xlsx'.format(date))
