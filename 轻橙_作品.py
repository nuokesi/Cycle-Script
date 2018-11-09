# -*- coding: utf-8 -*-
"""
Created on Fri Jun  1 19:34:11 2018

@author: Sun
"""

import zipfile
import glob
import os
import numpy as np
import pandas as pd
from python_cgtools import utils_sql
import smtplib
from email.header import Header
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import time


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
    address1 = "xiyou@66rpg.com"  # 芸豆
    address2 = "3001906487@qq.com"


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


db3406, cur3406 = utils_sql.get_conn_cur_by_class(utils_sql.AnalysisUser3406, _try_times=5)
cur3406.execute('''SELECT
                    	a.gindex,
                    	a.gname,
                       a.author_uid,
                    	a.fv_times,
                    	a.play_times,
                    	a.flower,
                    	release_word_sum,
                       share_times
                    FROM
                	      dbbh_website.org_game_summary a 
                    WHERE
                    	check_level >= 2  and gindex IN
             (
            		SELECT
            			gindex
            		FROM
            			dbbh_website.org_tag_game
            		WHERE
            			tid = 12337
            	) ''')
res = cur3406.fetchall()
ginfo1 = pd.DataFrame(list(res), columns=['gindex', '作品名称', '作者UID', '收藏', '人气', '鲜花', '字数', '分享'])

# 点赞
database = utils_sql.UgcUserOn3513
sql = """SELECT a.gindex,SUM(a.n) FROM ( select gindex,SUM(ticket_sum) as n from game_outer_attr.org_game_star_0 where gindex in {0} and star_type=5 GROUP BY gindex UNION ALL
select gindex,SUM(ticket_sum) as n from game_outer_attr.org_game_star_1 where gindex in {0} and star_type=5 GROUP BY gindex  UNION ALL
select gindex,SUM(ticket_sum) as n from game_outer_attr.org_game_star_2 where gindex in {0} and star_type=5 GROUP BY gindex  UNION ALL
select gindex,SUM(ticket_sum) as n from game_outer_attr.org_game_star_3 where gindex in {0} and star_type=5 GROUP BY gindex  UNION ALL
select gindex,SUM(ticket_sum) as n from game_outer_attr.org_game_star_4 where gindex in {0} and star_type=5 GROUP BY gindex  UNION ALL
select gindex,SUM(ticket_sum) as n from game_outer_attr.org_game_star_5 where gindex in {0} and star_type=5 GROUP BY gindex UNION ALL
select gindex,SUM(ticket_sum) as n from game_outer_attr.org_game_star_6 where gindex in {0} and star_type=5 GROUP BY gindex UNION ALL
select gindex,SUM(ticket_sum) as n from game_outer_attr.org_game_star_7 where gindex in {0} and star_type=5 GROUP BY gindex UNION ALL
select gindex,SUM(ticket_sum) as n from game_outer_attr.org_game_star_8 where gindex in {0} and star_type=5 GROUP BY gindex UNION ALL
select gindex,SUM(ticket_sum) as n from game_outer_attr.org_game_star_9 where gindex in {0} and star_type=5 GROUP BY gindex UNION ALL
select gindex,SUM(ticket_sum) as n from game_outer_attr.org_game_star_10 where gindex in {0} and star_type=5 GROUP BY gindex UNION ALL
select gindex,SUM(ticket_sum) as n from game_outer_attr.org_game_star_11 where gindex in {0} and star_type=5 GROUP BY gindex UNION ALL
select gindex,SUM(ticket_sum) as n from game_outer_attr.org_game_star_12 where gindex in {0} and star_type=5 GROUP BY gindex UNION ALL
select gindex,SUM(ticket_sum) as n from game_outer_attr.org_game_star_13 where gindex in {0} and star_type=5 GROUP BY gindex UNION ALL
select gindex,SUM(ticket_sum) as n from game_outer_attr.org_game_star_14 where gindex in {0} and star_type=5 ) as a WHERE a.gindex in {0} GROUP BY a.gindex""".format(
    tuple(list(ginfo1['gindex'])))
columns = ['gindex', '点赞']
ginfo2 = get_res(database, sql, columns)

# 昵称
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

# 标签
database = utils_sql.AnalysisUser3406
sql = """SELECT
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
        AND a.gindex in {gindex} 
        group by a.gindex""".format(gindex=tuple(list(ginfo1['gindex'])))
columns = ['gindex', '标签']
ginfo3 = get_res(database, sql, columns)

# 是否签约
db3408, cur3408 = utils_sql.get_conn_cur_by_class(utils_sql.UgcUserOn3408, _try_times=5)
sql = """ SELECT uid,'是' as '是否签约' FROM website_userinfo.org_user_sign WHERE `status` = 2 """
cur3408.execute(sql)
res = cur3408.fetchall()
sign = pd.DataFrame(list(res), columns=['作者UID', '是否签约'])

nick_name['作者UID'] = nick_name['作者UID'].astype(np.int64)
ginfo = pd.merge(ginfo1, ginfo2, on='gindex', how='left')
ginfo = pd.merge(ginfo, ginfo3, on='gindex', how='left')
ginfo = pd.merge(ginfo, nick_name, on='作者UID', how='left')
ginfo = pd.merge(ginfo, sign, on='作者UID', how='left')

ginfo.to_csv('P:\\a\\轻橙\\每日数据\\bygindex{0}.csv'.format(time.strftime("%Y%m%d")), sep=',', index=False)
ginfo.to_csv('P:\\a\\轻橙\\每日数据\\作品\\bygindex.csv', sep=',', index=False)

files = glob.glob('P:\\a\\轻橙\\每日数据\\作品\\*')
f = zipfile.ZipFile('P:\\a\\轻橙\\每日数据\\zuopinshuju.zip', 'w', zipfile.ZIP_DEFLATED)
for file in files:
    f.write(file, os.path.basename(file))
f.close()
send_email("轻橙作品数据", [Email_to.address1], "详见附件中<bygindex>表格",
           _attachment_path="P:\\a\\轻橙\\每日数据\\zuopinshuju.zip",
           _attachment_name=os.path.basename("P:\\a\\轻橙\\每日数据\\zuopinshuju.zip"))
