# -*- coding: utf-8 -*-
"""
Created on Tue Jul  3 10:31:41 2018

@author: jisi
"""

import os
import numpy as np
import pandas as pd

from cg_jisi.utils_mail import mail_send
from cg_jisi.utils_time import before_month
from cg_jisi.utils_sql import df_from_sql
from cg_jisi.utils_sql import UgcUserOn3501, UgcUserOn3402


def get_deal(_month):
    # 交易记录, 画师姓名
    sql = f"""SELECT
                a.order_id,
                a.buyer_id,
                a.seller_id,
                a.price*a.buy_num,
                a.buy_num,
                a.goods_name,
                FROM_UNIXTIME(a.pay_time,'%Y-%m-%d %H:%i:%s'),
                b.user_name
                FROM
                c2c_order.c2c_order_{_month} AS a ,
                c2c_user.user_identity AS b
                WHERE
                a.seller_id = b.uid AND
                a.pay_status = 1"""
    df = df_from_sql(UgcUserOn3501, sql, ['订单ID', '买家ID', '画师ID', '订单金额', '购买数量', '商品名称', '交易时间', '画师姓名'])
    return df


def get_nickname(df, st):
    # 获取昵称
    uid = tuple(df[f'{st}ID'])
    df = []
    for i in range(100):
        print(i)
        res = df_from_sql(UgcUserOn3402, f"SELECT uid,nick_name FROM dbbh_passport.uc_user_info_{i} WHERE uid in {uid}", [f'{st}ID', f'{st}昵称'])
        df.append(res)
    df = pd.concat(df, ignore_index=True)
    return df


def combine(df, df2, st):
    if type(df2[f'{st}ID'][0]) != np.int64:
        df2[f'{st}ID'] = df2[f'{st}ID'].astype(np.int64)
    df = pd.merge(df, df2, on=f'{st}ID', how='left')
    return df


if __name__ == '__main__':

    n = before_month(-1)
    path = f"P://a/素材交易/月报/素材交易月报{n}.xlsx"
    
    deal = get_deal(n)
    nickname_seller = get_nickname(deal, '画师')
    nickname_buyer = get_nickname(deal, '买家')
    deal = combine(deal, nickname_seller, '画师')
    deal = combine(deal, nickname_buyer, '买家')
    deal = deal[['画师昵称', '画师姓名', '画师ID', '买家昵称', '买家ID', '订单ID', '商品名称', '购买数量', '订单金额', '交易时间']]
    deal.to_excel(path, index=False)
    mail_send("素材交易月报{0}".format(n), ["2850774122@qq.com"], _attachment_path=path, _attachment_name=os.path.basename(path))

# 冰凝["2850774122@qq.com"]
