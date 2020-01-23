# -*- coding: utf-8 -*-
"""
Created on Thu Jan 23 10:40:13 2020

@author: Rameshkumar
"""

# import required python libraries  
import pandas as pd
#import xlrd
#import datetime
#from xlrd import open_workbook, cellname
import os
#import csv
#right input file
search_str = '_with_ann'

#create master dataframe to store the data


def temp_dataframe(file,year):
    temp_df = pd.read_csv(file, low_memory=False)
    old_names = temp_df.columns.tolist()
    #print(old_names)
    new_names = []
    for item in old_names:
        if 'GEO.' in item:
            new_names.append(item)
        else:
            new_names.append(item+"_"+year)
    #print(new_names)
    temp_df.rename(columns=dict(zip(old_names, new_names)), inplace=True)
    #print("temp dataframe shape :", temp_df.shape)       
    return temp_df

master_df = None
folder_name = ""
file_list = []
count =0
# data files location 
data_loc = "C:\\PerformX\\Data_processing\\Project_9\\Data\\"
output_loc = "C:\\PerformX\\Data_processing\\Project_9\\"

for root, dirs, files in os.walk(data_loc):
    for file in files:
        #print(file)
        str_file = str(file)
        if (file.endswith(".csv")) and (search_str in file):
            filename = os.path.join(root, file)
            words = str_file.split('_')
            year = '20'+ words[1]
            print(filename+" ....Processing")
            file_list.append(filename)
            chk_name = str(filename).split('\\')[-3]
            df_name = chk_name.replace(' ','_').replace('-','_')
                       
            if(folder_name != "") and (folder_name != chk_name):
                name = file_list[-2].split('\\')
                df_name= name[-3].replace(' ','_').replace('-','_')
                output_filename = output_loc + df_name +".csv"
                master_df.to_csv(output_filename,index = False)
                master_df = None
            
            updated_df = temp_dataframe(filename,year)
            
            if(master_df is None):
                master_df = updated_df.copy()
                #print("master dataframe shape:",master_df.shape)
                updated_df = None
                folder_name = chk_name
            else:
                #updated_df.drop(['GEO.id2','GEO.display-label'],axis=1,inplace=True)
                master_df = master_df.merge(updated_df,'outer',left_on=['GEO.id','GEO.id2','GEO.display-label'], right_on=['GEO.id','GEO.id2','GEO.display-label'])
                #print("master dataframe shape:",master_df.shape)
                updated_df = None
            
# write final dataframe           
output_filename =output_loc + df_name +".csv"
if os.path.isfile(output_filename) == False:
    master_df.to_csv(output_filename,index = False)
    master_df = None
    
print("Data processing completed")
           
            
            
            
            
            
            
            