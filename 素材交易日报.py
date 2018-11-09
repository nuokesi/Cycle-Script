# -*- coding: utf-8 -*-
"""
Created on Tue Jul  3 10:31:41 2018

@author: Sun
"""

import os
import glob
import time
import smtplib
import zipfile
import numpy as np
import pandas as pd
from email.header import Header
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from python_cgtools import utils_sql
from python_cgtools import utils_date


def get_res(_database, _sql, _columns):
    db, cur = utils_sql.get_conn_cur_by_class(_database, _try_times=5)
    cur.execute(_sql)
    res = cur.fetchall()
    data = pd.DataFrame(list(res), columns=_columns)
    return data


# 轻橙总工程数


# 我的邮箱555
class Email(object):
    name = "橙光data"
    smtpserver = "smtp.exmail.qq.com"
    address = "jisi@66rpg.com"
    password = "Abcdzyx2"


#    smtpserver = "smtp.163.com"
#    address = "shuju66@163.com"
#    password = "19891020zq"

class Email_to(object):
    address1 = "2850774122@qq.com"  # 冰凝
    address2 = "3001906487@qq.com"


#    address1 = "1105525044@qq.com"
#    address2 = 'jisi@66rpg.com'


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
    message = MIMEMultipart('mixed')
    message["From"] = Header(_class.name + "<" + _class.address + ">", charset="utf-8")
    # 群发邮件显示收件人时会有轻微的格式错乱，尚未找到合适的办法，但不影响发送
    # message["To"] = Header(", ".join(map(add_email_username, _receivers)), charset="utf-8")
    message["To"] = Header(",".join(_receivers), charset="utf-8")
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


database = utils_sql.UgcUserOn3501
sql = '''
        SELECT category_id,id,goods_name,
        (CASE WHEN category_id=1 THEN '立绘 '
        when category_id = 2 THEN '背景'
        when category_id = 3 THEN 'CG封面'
        when category_id = 4 THEN '小物件'
        when category_id = 5 THEN '特效'
        when category_id = 6 THEN 'UI'
        when category_id = 7 THEN '明星立绘' ELSE '未分类' end ) as kind,stock_num,price
        from c2c_goods.c2c_goods_list        '''
columns = ['category_id', '素材ID', '素材名称', '素材分类', '库存', '定价']
good_info = get_res(database, sql, columns)

date1 = time.strftime("%Y%m%d")
date = int(utils_date.date_before(date1, n=1, _date_str_style='%Y%m%d'))

# date = 20180804

database = utils_sql.UgcUserOn3501
sql = '''
SELECT  goods_id,seller_id,
        (CASE WHEN source =0 THEN '网站'
        when source = 1 THEN 'ios'
        when source = 2 THEN '安卓'
        when source = 4 THEN '手机站'
        when source = 5 THEN '工具'
         ELSE '未标记来源' end ) as source,
        count(*),SUM(price*buy_num),COUNT(DISTINCT buyer_id),sum(buy_num) from c2c_order.c2c_order_{0} WHERE pay_status =1 and FROM_UNIXTIME(pay_time,'%Y%m%d') = {1}
        group by goods_id,seller_id,source
        '''.format(str(date)[0:6], date)
columns = ['素材ID', '画师ID', '来源', '交易次数', '销售额', '购买顾客数', '销售数量']
df_good = get_res(database, sql, columns)

uid_need = list(df_good['画师ID'])

nick_name = pd.DataFrame(columns=['画师ID', '画师昵称'])
for i in range(0, 100):
    print(i)
    db3402, cur3402 = utils_sql.get_conn_cur_by_class(utils_sql.UgcUserOn3402, _try_times=5)
    sql = """SELECT uid,nick_name FROM dbbh_passport.uc_user_info_{0} WHERE uid in {1} """.format(i, tuple(uid_need))
    cur3402.execute(sql)
    res = cur3402.fetchall()
    nick_name_data = pd.DataFrame(list(res), columns=['画师ID', '画师昵称'])
    nick_name = pd.concat([nick_name, nick_name_data], ignore_index=True)

database = utils_sql.UgcUserOn3501
sql = """ SELECT uid,user_name from c2c_user.user_identity WHERE uid in {0}  """.format(tuple(uid_need))
columns = ['画师ID', '画师姓名']
true_name = get_res(database, sql, columns)

nick_name['画师ID'] = nick_name['画师ID'].astype(np.int64)
df_user = pd.merge(df_good, nick_name, on='画师ID', how='left')
df_user = pd.merge(df_user, true_name, on='画师ID', how='left')
df_user = pd.merge(df_user, good_info, on='素材ID', how='left')
df_user['日期'] = date
df_user = df_user[['日期', '素材ID', '素材名称', '素材分类', '库存', '销售数量', '定价', '销售额', '购买顾客数', '来源', '画师ID', '画师昵称', '画师姓名']]

df_user.to_csv('P:\\a\\素材交易\\交易平台销售维度_{0}.csv'.format(date), sep=',', index=False)
df_user.to_csv('P:\\a\\素材交易\\shuju\\交易平台销售维度.csv', sep=',', index=False)

# 日期	顾客数	新客数	画师数	新增画师数	在售素材数	新增在售素材数


database = utils_sql.UgcUserOn3501
sql = '''
     SELECT {1} as d,COUNT(DISTINCT buyer_id) from c2c_order.c2c_order_{0} WHERE pay_status =1 and FROM_UNIXTIME(pay_time,'%Y%m%d') = {1}
        '''.format(str(date)[0:6], date)
columns = ['日期', '（当日交易）顾客数']
df_sale1 = get_res(database, sql, columns)

database = utils_sql.UgcUserOn3501
sql = """select table_name 
        from information_schema.tables 
        where table_schema='c2c_order' AND  table_name LIKE 'c2c_order_201%' """
db, cur = utils_sql.get_conn_cur_by_class(database, _try_times=5)
cur.execute(sql)
res = cur.fetchall()
data_list = [i[0] for i in res]  # 所有的表名
data_list.remove('c2c_order_201705_20170507_chiyu')
data_all = pd.DataFrame()
database = utils_sql.UgcUserOn3501
columns = ['date', 'buyer_id']
for i in data_list:
    print(i)
    sql = """
            SELECT FROM_UNIXTIME(pay_time,'%Y%m%d'),buyer_id from c2c_order.{data} WHERE pay_status =1 and FROM_UNIXTIME(pay_time,'%Y%m%d') <={d}
	    """.format(data=i, d=date)
    res = get_res(database, sql, columns)
    data_all = pd.concat([data_all, res], ignore_index=True)
data_all['date'] = data_all['date'].astype(int)
data_all_old = data_all[data_all['date'] < date]

df_sale2 = pd.DataFrame({'日期': ['{0}'.format(date)], '（当日交易）新客数': [
    pd.Series.nunique(data_all['buyer_id']) - pd.Series.nunique(data_all_old['buyer_id'])]},
                        columns=['日期', '（当日交易）新客数'])
df_sale2['日期'] = df_sale2['日期'].astype(int)
database = utils_sql.UgcUserOn3501
sql = '''
     SELECT {0} as date,COUNT(DISTINCT seller_id) from c2c_goods.c2c_goods_list WHERE FROM_UNIXTIME(put_on_time,'%Y%m%d')<= {0}

        '''.format(date)
columns = ['日期', '（全量素材对应的）画师数']
df_sale3 = get_res(database, sql, columns)

database = utils_sql.UgcUserOn3501
sql = '''
             SELECT
        	{0} as date,COUNT(DISTINCT seller_id)
        FROM
        	c2c_goods.c2c_goods_list
        WHERE
        	FROM_UNIXTIME(put_on_time, '%Y%m%d') <= {0}
        AND seller_id NOT IN (
        	SELECT DISTINCT
        		seller_id
        	FROM
        		c2c_goods.c2c_goods_list
        	WHERE
        		FROM_UNIXTIME(put_on_time, '%Y%m%d') < {0}
        )
        '''.format(date)
columns = ['日期', '（全量素材对应的）新增画师数']
df_sale4 = get_res(database, sql, columns)

database = utils_sql.UgcUserOn3501
sql = '''
     SELECT {0} as date,COUNT(*) from c2c_goods.c2c_goods_list WHERE  FROM_UNIXTIME(put_on_time,'%Y%m%d')<= {0}
        '''.format(date)
columns = ['日期', '总素材数']
df_sale5 = get_res(database, sql, columns)

database = utils_sql.UgcUserOn3501
sql = '''
     SELECT {0} as date,COUNT(*) from c2c_goods.c2c_goods_list WHERE goods_status IN (1,4) and FROM_UNIXTIME(put_on_time,'%Y%m%d')<= {0}
        '''.format(date)
columns = ['日期', '总在售材数']
df_sale6 = get_res(database, sql, columns)

database = utils_sql.UgcUserOn3501
sql = '''
     SELECT {0} as date,COUNT(*) from c2c_goods.c2c_goods_list WHERE  FROM_UNIXTIME(put_on_time,'%Y%m%d')= {0}
        '''.format(date)
columns = ['日期', '新增素材数']
df_sale7 = get_res(database, sql, columns)
df_sale = pd.merge(df_sale1, df_sale2, on='日期', how='left')
df_sale = pd.merge(df_sale, df_sale3, on='日期', how='left')
df_sale = pd.merge(df_sale, df_sale4, on='日期', how='left')
df_sale = pd.merge(df_sale, df_sale5, on='日期', how='left')
df_sale = pd.merge(df_sale, df_sale6, on='日期', how='left')
df_sale = pd.merge(df_sale, df_sale7, on='日期', how='left')

df_sale.to_csv('P:\\a\\素材交易\\交易平台用户维度_{0}.csv'.format(date), sep=',', index=False)
df_sale.to_csv('P:\\a\\素材交易\\shuju\\交易平台用户维度.csv', sep=',', index=False)

database = utils_sql.UgcUserOn3501
sql = '''
     SELECT {0} as date,seller_id,goods_name,stock_num,price,goods_status,
      (CASE WHEN category_id=1 THEN '立绘 '
        when category_id = 2 THEN '背景'
        when category_id = 3 THEN 'CG封面'
        when category_id = 4 THEN '小物件'
        when category_id = 5 THEN '特效'
        when category_id = 6 THEN 'UI'
        when category_id = 7 THEN '明星立绘' ELSE '未分类' end ) as kind
     from c2c_goods.c2c_goods_list WHERE  FROM_UNIXTIME(put_on_time,'%Y%m%d')= {0}
        '''.format(date)
columns = ['日期', '画师ID', '素材名称', '库存', '定价', '商品状态', '素材分类']
df_new_goods = get_res(database, sql, columns)
uid_need = list(df_new_goods['画师ID'])
nick_name = pd.DataFrame(columns=['画师ID', '画师昵称'])
for i in range(0, 100):
    print(i)
    db3402, cur3402 = utils_sql.get_conn_cur_by_class(utils_sql.UgcUserOn3402, _try_times=5)
    sql = """SELECT uid,nick_name FROM dbbh_passport.uc_user_info_{0} WHERE uid in {1} """.format(i, tuple(uid_need))
    cur3402.execute(sql)
    res = cur3402.fetchall()
    nick_name_data = pd.DataFrame(list(res), columns=['画师ID', '画师昵称'])
    nick_name = pd.concat([nick_name, nick_name_data], ignore_index=True)

database = utils_sql.UgcUserOn3501
sql = """ SELECT uid,user_name from c2c_user.user_identity WHERE uid in {0}  """.format(tuple(uid_need))
columns = ['画师ID', '画师姓名']
true_name = get_res(database, sql, columns)

nick_name['画师ID'] = nick_name['画师ID'].astype(np.int64)
df_new_goods = pd.merge(df_new_goods, nick_name, on='画师ID', how='left')
df_new_goods = pd.merge(df_new_goods, true_name, on='画师ID', how='left')
df_new_goods = df_new_goods[['日期', '画师ID', '画师昵称', '画师姓名', '素材名称', '素材分类', '库存', '定价', '商品状态']]

df_new_goods.to_csv('P:\\a\\素材交易\\交易平台新增素材_{0}.csv'.format(date), sep=',', index=False)
df_new_goods.to_csv('P:\\a\\素材交易\\shuju\\交易平台新增素材.csv', sep=',', index=False)

# 顾客角度交易日报

database = utils_sql.UgcUserOn3501
sql = '''
     SELECT {1} as d,buyer_id,seller_id,goods_id,sum(buy_num) from c2c_order.c2c_order_{0}
     WHERE pay_status =1 
     and FROM_UNIXTIME(pay_time,'%Y%m%d') = {1}
     group by d,buyer_id,seller_id,goods_id
        '''.format(str(date)[0:6], date)
columns = ['日期', '顾客ID', '画师ID', '素材ID', '购买数量']
df_sale_consumer = get_res(database, sql, columns)
df_sale_consumer["顾客ID"] = df_sale_consumer["顾客ID"].apply(lambda a: str(int(float(a))))
df_sale_consumer["画师ID"] = df_sale_consumer["画师ID"].apply(lambda a: str(int(float(a))))

uid_need = list(set(list(df_sale_consumer['画师ID']) + list(df_sale_consumer['顾客ID'])))

nick_name = pd.DataFrame(columns=['ID', '昵称'])
for i in range(0, 100):
    print(i)
    db3402, cur3402 = utils_sql.get_conn_cur_by_class(utils_sql.UgcUserOn3402, _try_times=5)
    sql = """SELECT uid,nick_name FROM dbbh_passport.uc_user_info_{0} WHERE uid in {1} """.format(i, tuple(uid_need))
    cur3402.execute(sql)
    res = cur3402.fetchall()
    nick_name_data = pd.DataFrame(list(res), columns=['ID', '昵称'])
    nick_name = pd.concat([nick_name, nick_name_data], ignore_index=True)
nick_name["ID"] = nick_name["ID"].apply(lambda a: str(int(float(a))))
database = utils_sql.UgcUserOn3501
sql = """ SELECT uid,user_name from c2c_user.user_identity WHERE uid in {0}  """.format(tuple(uid_need))
columns = ['ID', '姓名']
true_name = get_res(database, sql, columns)
true_name["ID"] = true_name["ID"].apply(lambda a: str(int(float(a))))

df_sale_consumer = pd.merge(df_sale_consumer, nick_name, left_on="顾客ID", right_on="ID", how='left')
df_sale_consumer.rename(columns={'昵称': '顾客昵称'}, inplace=True)
del df_sale_consumer['ID']
df_sale_consumer = pd.merge(df_sale_consumer, nick_name, left_on="画师ID", right_on="ID", how='left')
df_sale_consumer.rename(columns={'昵称': '画师昵称'}, inplace=True)
del df_sale_consumer['ID']
df_sale_consumer = pd.merge(df_sale_consumer, true_name, left_on="画师ID", right_on="ID", how='left')
df_sale_consumer.rename(columns={'姓名': '画师姓名'}, inplace=True)
del df_sale_consumer['ID']
df_sale_consumer = pd.merge(df_sale_consumer, good_info, on="素材ID", how='left')
df_sale_consumer = df_sale_consumer[
    ['日期', '顾客ID', '顾客昵称', '素材分类', '素材名称', '素材ID', '定价', '购买数量', '画师ID', '画师昵称', '画师姓名']]

df_sale_consumer.to_csv('P:\\a\\素材交易\\交易平台顾客角度_{0}.csv'.format(date), sep=',', index=False)
df_sale_consumer.to_csv('P:\\a\\素材交易\\shuju\\交易平台顾客角度.csv', sep=',', index=False)

# 数据截点素材情况

database = utils_sql.UgcUserOn3501
sql = '''
        SELECT category_id,id,goods_name,
        (CASE WHEN category_id=1 THEN '立绘 '
        when category_id = 2 THEN '背景'
        when category_id = 3 THEN 'CG封面'
        when category_id = 4 THEN '小物件'
        when category_id = 5 THEN '特效'
        when category_id = 6 THEN 'UI'
        when category_id = 7 THEN '明星立绘' ELSE '未分类' end ) as kind,stock_num,price,goods_num,sales_count,seller_id
        from c2c_goods.c2c_goods_list        '''
columns = ['category_id', '素材ID', '素材名称', '素材分类', '库存', '定价', '商品总数量', '总销售数量', '画师ID']
good_info_all = get_res(database, sql, columns)
good_info_all['日期'] = date
uid_need = list(set(list(good_info_all['画师ID'])))
len(uid_need)
nick_name = pd.DataFrame(columns=['画师ID', '昵称'])
for i in range(0, 100):
    print(i)
    db3402, cur3402 = utils_sql.get_conn_cur_by_class(utils_sql.UgcUserOn3402, _try_times=5)
    sql = """SELECT uid,nick_name FROM dbbh_passport.uc_user_info_{0} WHERE uid in {1} """.format(i, tuple(uid_need))
    cur3402.execute(sql)
    res = cur3402.fetchall()
    nick_name_data = pd.DataFrame(list(res), columns=['画师ID', '昵称'])
    nick_name = pd.concat([nick_name, nick_name_data], ignore_index=True)
# nick_name["ID"] = nick_name["ID"].apply(lambda a : str(int(float(a))))
database = utils_sql.UgcUserOn3501
sql = """ SELECT uid,user_name from c2c_user.user_identity WHERE uid in {0}  """.format(tuple(uid_need))
columns = ['画师ID', '姓名']
true_name = get_res(database, sql, columns)
# true_name["ID"] = true_name["ID"].apply(lambda a : str(int(float(a))))
nick_name['画师ID'] = nick_name['画师ID'].astype(np.int64)
good_info_all = pd.merge(good_info_all, nick_name, on='画师ID', how='left')
good_info_all = pd.merge(good_info_all, true_name, on='画师ID', how='left')
del good_info_all['category_id']

good_info_all.to_csv('P:\\a\\素材交易\\所有素材_{0}.csv'.format(date), sep=',', index=False)
good_info_all.to_csv('P:\\a\\素材交易\\shuju\\所有素材.csv', sep=',', index=False)

files = glob.glob('P:\\a\\素材交易\\shuju\\*')
f = zipfile.ZipFile('P:\\a\\素材交易\\shuju.zip', 'w', zipfile.ZIP_DEFLATED)
for file in files:
    f.write(file, os.path.basename(file))
f.close()
send_email("{0}素材交易平台每日数据".format(date), [Email_to.address1], "详见附件中对应表格，jiashuju.csv不用查看",
           _attachment_path="P:\\a\\素材交易\\shuju.zip", _attachment_name=os.path.basename("P:\\a\\素材交易\\shuju.zip"))
