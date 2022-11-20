import pandas as pd
from bs4 import BeautifulSoup
import re
import os
import zipfile
import shutil
import multiprocessing
import sqlite3



def html_preprocess(file_name,comp,qtr):
    with open(file_name, encoding="UTF-8") as f:
        data=f.read()
    soup = BeautifulSoup(data,'html.parser')

    data_all=[[] for i in range(6)]

    for sht in range(3):
        sheet=soup.find_all('table')[sht]
        cate=sheet.find_all('tr')[0].find_all('th')[0].find(class_='zh').text.strip()

        i=1
        while i< len(sheet.find_all('tr')):
            if i==1:
                time_window=[]
                for time in sheet.find_all('tr')[i].find_all('th')[2:]:
                    time_window.append(time.find(class_='zh').text)

            elif sheet.find_all('tr')[i].find_all('td')[0].text=='' and re.search(r'\d',sheet.find_all('tr')[i+1].find_all('td')[0].text):
                sub_cate=sheet.find_all('tr')[i].find_all('td')[1].find(class_='zh').text.strip()
                subject_code=sheet.find_all('tr')[i+1].find_all('td')[0].text

            elif re.search(r'\d',sheet.find_all('tr')[i].find_all('td')[0].text):

                for j,n in enumerate(sheet.find_all('tr')[i].find_all('td')[2:]):
                    data_all[0].append(cate)
                    data_all[1].append(sub_cate)
                    data_all[2].append(sheet.find_all('tr')[i].find_all('td')[1].find(class_='zh').text.strip())
                    data_all[3].append(time_window[j])
                    data_all[4].append(n.text)
                    data_all[5].append(subject_code)
            i+=1

    table=pd.DataFrame()
    for i,col in enumerate(['Category','Sub-Category','Subject','Period','$','Subject Code']):
        table[col]=data_all[i]

    table['Comp']=comp
    table['Qtr']=qtr

    conn=sqlite3.connect('test')
    table.to_sql('CN_HTML_DF',conn,if_exists='append')
    conn.commit()
    conn.close()

def xml_preprocess(file_name):
    xml_df=pd.read_xml(file_name)
    xml_df.dropna(subset=['unitRef'],inplace=True)
    ui=xml_df.columns.to_list().index('unitRef')
    target_col=xml_df.columns[ui+1:]
    xml_df['Subject']=pd.NA
    xml_df['$']=pd.NA

    for col in target_col:
        xml_df.loc[~xml_df[col].isna(),'Subject']=col
        xml_df['$'].fillna(xml_df[col],inplace=True)
        xml_df.drop(col,axis=1,inplace=True)

    xml_df=xml_df[['contextRef','Subject','$']]
    xml_df['contextRef']=xml_df['contextRef'].replace(r'From','',regex=True)

    return xml_df

def file_process(f):
    print(f)
    root=r'C:\Users\s3309\Desktop\Investment\html_files'
    filename=f.split('.')[0]
    filename=filename.split('-')
    comp=filename[-2]
    qtr=filename[-1]
    abspath=os.path.join(root,f)

    try:
        if f.endswith('.html'):            
            temp=html_preprocess(abspath,comp,qtr)
            shutil.move(abspath,os.path.join(r'C:\Users\s3309\Desktop\Investment\completed',f))
    except Exception as e:
        shutil.move(abspath,os.path.join(r'C:\Users\s3309\Desktop\Investment\problem',f))
        print(e)
        return None

    return None