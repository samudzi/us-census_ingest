# -*- coding: utf-8 -*-
"""
Created on Fri Jan 24 13:59:34 2020

@author: Rameshkumar
"""
# import required python libraries  
import pandas as pd
import os


def temp_dataframe(file,year):
    temp_df = pd.read_csv(file,low_memory=False)
    old_names = temp_df.columns.tolist()
    #print(old_names)
    new_names = []
    cols_notformat = ['State','County','Region','Division','County_name']
    for item in old_names:
        if item in cols_notformat:
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
data_loc = "C:\\PerformX\\Data_processing\\Project_9\\Data\\Build Permits\\"
# output file location 
output_loc = "C:\\PerformX\\Data_processing\\Project_9\\"

for root, dirs, files in os.walk(data_loc):
    for file in files:
        #print(file)
        str_file = str(file)
        if (file.endswith(".csv")):
            filename = os.path.join(root, file)
            year = str(''.join(filter(str.isdigit, str_file)))
            print(filename+" ....Processing")
            folder_name = str(filename).split('\\')[-2]
            updated_df = temp_dataframe(filename,year)
            
            if(master_df is None):
                master_df = updated_df.copy()
                #print("master dataframe shape:",master_df.shape)
                updated_df = None
                
            else:
                #updated_df.drop(['GEO.id2','GEO.display-label'],axis=1,inplace=True)
                master_df = master_df.merge(updated_df,'outer',left_on=['State','County','Region','Division','County_name'], right_on=['State','County','Region','Division','County_name'])
                #print("master dataframe shape:",master_df.shape)
                updated_df = None
            
# write final dataframe           
output_filename =output_loc + folder_name +".csv"
if os.path.isfile(output_filename) == False:
    master_df.drop(master_df.index[0],inplace=True)
    master_df.to_csv(output_filename)
    master_df = None
           
print("Data processing completed")            
       