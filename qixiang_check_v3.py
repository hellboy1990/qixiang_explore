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
        df=pd.concat([df,df_i],sort=False)
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
    datacolumnslist = [list(datas[i]) for i in datacolumns]
    # 提取站点唯一值并建立列表
    stations=[]
    for i in range(len(datacolumnslist[0])):
        if datacolumnslist[0][i] not in stations:
            stations.append(datacolumnslist[0][i])
    # 建立站点数据列表_逐日
    qixiangday = ['timeavailas', 'day', 'station_n_tem_ave', 'station_n_tem_ave_max','station_n_tem_ave_min',
                  'station_n_shuiqiya_ave', 'station_n_jiangshui_20','station_n_jiangshui_08','station_n_fengsu_ave']
    qixiangdaylist = [[] for i in range(len(qixiangday))]
    station_n_tem_sum = [[] for i in range(len(jiwens))]  # 注意积温列表数量
    # 建立临时列表,临时列表同一站点的逐日数据
    qixiangtemp = [i + '_temp' for i in qixiangday]
    qixiangtemplist = [[[] for i in range(len(stations))] for i in range(len(qixiangtemp))]
    # 根据站点列表来提取气象数据，每个站点拥有列表数据而不是唯一值，需要计算平均值
    for i in range(len(stations)):  # 遍历站点列表
        for j in range(len(datacolumnslist[0])):
            if datacolumnslist[0][j] == stations[i]:  # 如果站名相同则写入其它气象数据
                for x in range(len(qixiangtemplist)):
                    if datacolumnslist[x+1][j] not in [999998,999999]:  # 去除缺省值
                        qixiangtemplist[x][i].append(datacolumnslist[x+1][j])
    #print(qixiangtemplist[2][0])
    # 下一步计算平均值并添加坐标
    file_n = filecsv.replace(".csv", "_N.csv")
    print(file_n)
    #print(len(qixiangtemplist[2][0]))
    # 将站点的多时相数据保存为平均值数据
    for i in range(len(qixiangtemplist)):  # 九列
        for j in range(len(qixiangtemplist[i])):  # 遍历每行数据
            # print(qixiangtemplist[i][j])
            if len(qixiangtemplist[i][j]) != 0:  # 不为空值
                qixiangdaylist[i].append(sum(qixiangtemplist[i][j]) / len(qixiangtemplist[i][j]))
    # 求活动积温
    for x in range(len(jiwens)):
        tem_sum=[]
        for i in range(len(qixiangtemplist[2])):
            tems=[tem for tem in qixiangtemplist[2][i] if tem >= jiwens[x]]
            tem_sum.append(sum(tems))
        #print(len(tem_sum))
        qixiangday.append('jiwen%s' % jiwens[x])  # 在气象列表中插件积温列
        qixiangdaylist.append(tem_sum)  # 在气象列表中插件积温数据
    # 这里加入站点坐标
    stations_china = list(pd.read_excel(file_station, sheet_name='Sheet1')[u'区站号'])
    lng = list(pd.read_excel(file_station, sheet_name='Sheet1')[u'经度'])
    lat = list(pd.read_excel(file_station, sheet_name='Sheet1')[u'纬度'])
    lngs,lats=[],[]
    for i in range(0, len(stations_china)):
        # 符合条件则存入临时列表
        for j in range(0, len(stations)):
            if stations[j] == stations_china[i]:
                print(stations[j])
                lngs.append(lng[i])  # 经度
                lats.append(lat[i])  # 纬度
    qixiangday.insert(0,'station_n')
    qixiangday.extend(['lngs','lats'])
    qixiangdaylist.insert(0,stations)
    qixiangdaylist.extend([lngs,lats])
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
    folder = "D:\\Database\\02China\\04Qixiang\\510000_Sichuan\\S201912311342215254400\\"
    #folder="C:\\Users\\jli\\Desktop\\AAA"
    step1=input('是否进行文件转换:')
    if int(step1)==0:
        filecsv=transfer_data(folder)
        #filecsv= "D:\\Database\\02China\\04Qixiang\\510000\\SURF_CHN_MUL_MDAY_19812010.csv"
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