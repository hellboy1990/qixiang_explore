#!/usr/bin/env python
# -*- coding:utf-8 -*-


import os
import pandas as pd
from datetime import datetime
from txt_table import txt_table
import re


# 将txt文件转化为csv
def transfer_data(folder):
    #建立使用文件列表
    filenames=[]
    filelist=[]
    filexlsx=[]
    #遍历文件寻找txt
    files=os.walk(folder)
    for root,dirs,files in files:
        for file in files:
            # 自动生成文件名
            if "readme" in file:
                filexls=re.match("(.*)_readme",file).group(1)
                filexlsx.append(filexls)
            else:
                pass
            filename=os.path.join(root,file)
            filenames.append(filename)
    for filename in filenames:
        try:
            if ".txt" in filename and "readme" not in filename:
                fileout=filename.replace('.txt','.csv')
                data=txt_table(filename)
                data.to_csv(fileout,sep=',',encoding='utf-8')
                filelist.append(fileout)
            elif ".TXT" in filename and "readme" not in filename:
                fileout = filename.replace('.TXT','.csv')
                data=txt_table(filename)
                data.to_csv(fileout,sep=',',encoding='utf-8')
                filelist.append(fileout)
        except:
            print('error!')
            continue
    print('已将txt文件转化为csv!')
    #将数据合并
    #print(filexlsx[0])
    fileouts=os.path.join(folder,(filexlsx[0]+'.csv'))
    df=pd.DataFrame()
    for i in range(0,len(filelist)):
        df_i=pd.read_csv(filelist[i],sep=',',encoding='utf-8')
        #print(df_i)
        #df.append(df_i)
        df=pd.concat([df,df_i])
    df.to_csv(fileouts,sep=',',encoding='utf-8')
    print(df.head())
    print("已完成数据合并!")
    return fileouts


# 提取相关数据
def extract_data(filecsv,jiwens,file_station):
    # 读取气象数据,这一步要提取有用的气象数据并求平均值
    datas = pd.read_csv(filecsv, sep=',', encoding='utf-8')
    # 提取列名
    datacolumns= list(datas.columns)[2:]
    del datacolumns[2]  # 删除日期
    #print(datacolumns)
    datacolumnslist = [list(datas[i]) for i in datacolumns]
    #print(datacolumnslist[0])
    # shidu_ave = list(datas[u'V13003_701'])
    file_n = filecsv.replace(".csv", "_N.csv")
    print(file_n)
    stations_china = list(pd.read_excel(file_station, sheet_name='Sheet1')[u'区站号'])
    lng = list(pd.read_excel(file_station, sheet_name='Sheet1')[u'经度'])
    lat = list(pd.read_excel(file_station, sheet_name='Sheet1')[u'纬度'])
    # 建立站点数据列表
    qixiangday=['stations_n','timeavailas','lngs','lats','station_n_tem_ave','station_n_tem_ave_max','station_n_tem_ave_min',
                  'station_n_shuiqiya_ave','station_n_jiangshui_20','station_n_jiangshui_08','station_n_fengsu_ave']
    qixiangdaylist=[[] for i in range(len(qixiangday))]
    #print(qixiangdaylist)
    station_n_tem_sum = [[] for i in range(len(jiwens))]
    for i in range(0, len(stations_china)):
        # 临时列表
        qixiangtemp = ['station_n','timeavaila', 'lng_n', 'lat_n', 'station_tem_ave', 'station_tem_ave_max','station_tem_ave_min',
                       'station_shuiqiya_ave', 'station_jiangshui_20', 'station_jiangshui_08','station_fengsu_ave']
        qixiangtemplist=[[] for i in range(len(qixiangtemp))]
        # 符合条件则建立列表
        for j in range(0, len(datacolumnslist[0])):
            if datacolumnslist[0][j] == stations_china[i]:
                print(datacolumnslist[0][j])
                qixiangtemplist[0].append(datacolumnslist[0][j])  # 区站号
                qixiangtemplist[1].append(datacolumnslist[1][j])  # 有效时段
                qixiangtemplist[2].append(lng[i])  # 经度
                qixiangtemplist[3].append(lat[i])  # 纬度
                qixiangtemplist[4].append(datacolumnslist[2][j])  # 累年日平均气温
                qixiangtemplist[5].append(datacolumnslist[3][j])  # 累年平均日最高气温
                qixiangtemplist[6].append(datacolumnslist[4][j])  # 累年平均日最低气温
                qixiangtemplist[7].append(datacolumnslist[5][j])  # 累年日平均水汽压
                qixiangtemplist[8].append(datacolumnslist[6][j])  # 累年20-20时日降水量
                qixiangtemplist[9].append(datacolumnslist[7][j])  # 累年08-08时日降水量
                qixiangtemplist[10].append(datacolumnslist[8][j])  # 累年日平均风速
        #print(qixiangtemplist[9])
        if len(qixiangtemplist[4]) != 0:
            #print(qixiangtemplist[1])
            qixiangdaylist[0].append(qixiangtemplist[0][0])  # 区站号
            qixiangdaylist[1].append(str(qixiangtemplist[1][0]))  # 有效时段
            qixiangdaylist[2].append(qixiangtemplist[2][0])  # 经度
            qixiangdaylist[3].append(qixiangtemplist[3][0])  # 纬度
            # 求平均值
            qixiangdaylist[4].append(sum(qixiangtemplist[4]) / len(qixiangtemplist[4]))  # 累年日平均气温
            qixiangdaylist[5].append(sum(qixiangtemplist[5]) / len(qixiangtemplist[5]))  # 累年平均日最高气温
            qixiangdaylist[6].append(sum(qixiangtemplist[6]) / len(qixiangtemplist[6]))  # 累年平均日最低气温
            qixiangdaylist[7].append(sum(qixiangtemplist[7]) / len(qixiangtemplist[9]))  # 累年日平均水汽压
            qixiangdaylist[8].append(sum(qixiangtemplist[8]) / len(qixiangtemplist[8]))  # 累年20-20时日降水量
            qixiangdaylist[9].append(sum(qixiangtemplist[9]) / len(qixiangtemplist[9]))  # 累年08-08时日降水量
            qixiangdaylist[10].append(sum(qixiangtemplist[10]) / len(qixiangtemplist[10]))  # 累年日平均风速
            # 求活动积温
            for x in range(len(jiwens)):
                tem_sum = []
                for tem in qixiangtemplist[4]:
                    if tem > jiwens[x]:
                        tem_sum.append(tem)
                    else:
                        pass
                station_n_tem_sum[x].append(sum(tem_sum))
    #print(qixiangdaylist[10])
    # 建立积温更新后的列表
    for i in range(len(jiwens)):
        qixiangday.append('jiwen%s' % jiwens[i])
        qixiangdaylist.append(station_n_tem_sum[i])
    dfs=pd.DataFrame(qixiangdaylist)
    dfs=dfs.T
    dfs.columns=qixiangday
    print(dfs.head())
    dfs.to_csv(file_n, sep=',', encoding='utf-8')
    print('已完成气象数据提取!')

if __name__=='__main__':
    time_start = datetime.now()
    print('开始时间:' + str(time_start))
    '''第一步，将txt文件转化为csv'''
    folder = "D:\\Database\\02China\\04Qixiang\\510000\\"
    #folder="C:\\Users\\jli\\Desktop\\AAA"
    step1=input('是否进行文件转换:')
    if int(step1)==0:
        #filecsv=transfer_data(folder)
        filecsv= "D:\\Database\\02China\\04Qixiang\\510000\\SURF_CHN_MUL_MDAY_19812010.csv"
        '''第二步，提取每个站点的数据'''
        step2=input("是否提取站点数据:")
        if int(step2)==0:
            file_station="D:\\Database\\02China\\04Qixiang\\SURF_CHN_MUL_STATION.xlsx"
            jiwens=[0]
            extract_data(filecsv, jiwens, file_station)
    time_end = datetime.now()
    print('结束时间:' + str(time_end))
    time_last = time_end - time_start
    print('用时' + str(time_last))