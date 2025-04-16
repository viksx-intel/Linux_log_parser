
import streamlit as st
import pandas as pd
import os
import zipfile
import zlib
import difflib
from difflib import get_close_matches
import nltk
from nltk.tokenize import word_tokenize
import numpy as np
import time
import datetime
import re
import paramiko
import select
from paramiko import SSHClient, AutoAddPolicy

STREAMLIT=1

MAX_ROWS = 1000000
MAX_DISP = 500000

project_path =  os.getcwd()
SIMILARITY_SCORE = 0.90

FLAG_DPMO=1
FLAG_DPMT=2

DPMO = 1
DPMT = 2
DMESG = 3
SYSLOG = 4

log_lines_dmesg=0
log_lines_syslog=0
log_lines_dpmt=0
log_lines_total=0

log_files_dmesg=0
log_files_syslog=0
log_files_dpmt=0
log_files_total=0

log_lines = [0]*4



VERSION = 2

category=['','DPMO','DPMT']
sheets=['DPMO','DPMT']
stats_df=pd.DataFrame(columns=['Start Time: ','End Time: ','Lapsed Time: ','DPMO Folders: ','DPMT Folders: ','DMESG Folders: ','SYSLOG Folders: ','Other Folders: ','Total Folders: ','Total Log Lines: ','Error Log Lines: ','Percent Errors: '])
stats_df.loc[0,'Total Log Lines: '] = 0
stats_df.loc[0,'Error Log Lines: '] = 0


total_no_of_folders = 0

# Define the failure and affirmative keywords
failure_keywords =  [
                        "fail", "MissingConfigError", "TimeoutError", "UnknownPluginError", "No such", 
                        "ERROR", "AttributeError", "ModuleNotFoundError", "Assertion error", "TypeError", 
                        "ValueError", "NameError", "RuntimeError", "Valueerror", "EmptyTable", "IPC_Error", 
                        "not found", "error", "failed", "critical","EmptyTable","IndexError", "not"
                    ]

variables=[
           "ts", "info", "comp", "pci_id", "msg", 
           "err", "error", "warning", "acpi_id", "notice", 
           "debug", "command", "code", "event", "commit", "url", 
           "version"
          ]

v = set(variables)
variables = list(v)

comp_variables = [
                    'acpi','alarm','aspm','audio','boot','bt','camera','charger','cnvi','codec','coreboot','cpu','cr50','cse','d3','dc6','dma','dmic',
                    'dock', 'drm', 'ec','firmware','fsp','gfx','gpio','hdmi','i2c','kernel','lid','logs/kernel','lpss','media','mei','memory','nvme','pc10','pch','pci',
                    'pcie','perf','pg','pmc','power','retimer','runtimepm','s0ix','s3','sd','sensor','slp_s0','soundwire','spi','ssd','tbt','thermal'
                    'touch','tpm','usb','vbt','wifi','wwan'
                 ]

c = set(comp_variables)
comp_variables = list(c)

additional_variables = variables
additional_comp_variables = comp_variables

def get_file_names(file_name,n_rows):
    quo = n_rows//MAX_ROWS
    index = 0
    file_names=[]
    start_indexes=[]
    end_indexes = []
    if(quo>0):
        for r in range(1,quo+1):
            file_names.append(file_name+"_"+str(r)+".xlsx")
            start_indexes.append(index)
            index = index+MAX_ROWS
            end_indexes.append(index)
        rem = n_rows%MAX_ROWS
        if(rem>0):
            file_names.append(file_name+"_"+str(r+1)+".xlsx")
            start_indexes.append(index)
            index=index+rem
            end_indexes.append(index)
    else:
        file_names.append(file_name+".xlsx")
        start_indexes.append(index)
        end_indexes.append(index+n_rows)
    return(file_names,start_indexes,end_indexes)


low_prirority_words = set(['error','err','fail','failed'])
def get_additional_var(additional_variables,additional_comp_variables,s):
    var=""
    comp_var=""
    v=[]
    for x in additional_variables:
        if x in s.lower():
            v.append(x)
    if(len(v)>0):
        ss=[]
        if(len(v)==1):
            ss = v[0]
        if(len(v)>1):
            ss = list(sorted(set(v) - low_prirority_words))       
        var = v[0] 
        if(len(ss)>=1):
            var = ss[0]
        #t=additional_variables.index(var)
    c=[]
    for r in additional_comp_variables:
        if(r in s.lower()):
            c.append(r)
    if(len(c)>0):
        u = sorted(c)
        comp_var=u[0]
    return(var,comp_var)

failure_keywords = list(map(str.lower,failure_keywords))
affirmative_keywords = ["success", "successful", "completed"]

# Master error file operations
project_path = os.getcwd()
file_name = project_path + "//DPMT_DPMO_failure_error_listing.xlsx"

df_dpmt = pd.read_excel(file_name, sheet_name='DPMT')
df_dpmo = pd.read_excel(file_name, sheet_name='DPMO')
df_dpmt_hsd = pd.read_excel(file_name, sheet_name='dpmt_hsd')
df_dpmo_hsd = pd.read_excel(file_name, sheet_name='dpmo_hsd')

df_dpmo_mt = pd.concat([df_dpmo, df_dpmt], axis=0)

error_string   = list(df_dpmo_mt['Error string'].str.lower())
#variables      = list(df_dpmo_mt['Variables'])
#comp_variables = list(df_dpmo_mt['Comp Variables'])



append_df = pd.DataFrame(columns = df_dpmo_mt.columns)

######################################### Server Functions ##################################################

def get_folder_from_server(channel,client,path):
  temp_path = 'find ' + path + ' "*"'
  stdin, stdout, stderr = client.exec_command(temp_path)
  folders=[]
  for line in stdout:
      temp = line.strip('\n')
      folder, name = '/'.join(temp.split('/')[:-1]), temp.split('/')[-1]
      #print(temp)
      #print('folder=',folder,'and file=',name,'\n')
      folders.append(temp)
  return(folders) 

#Remote Connection to server
def remote_connection(host, username, password):
  client = paramiko.client.SSHClient()
  client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
  client.connect(host, username=username, password=password)
  transport = client.get_transport()
  channel = transport.open_session()
  return(channel,client)


#ssh_client = paramiko.SSHClient()
#ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#ssh_client.connect(hostname=host,username=username,password=password)
#ftp_client= ssh_client.open_sftp()
######################################### Server Functions ##################################################



def update_delta_error_in_master_file(df,sheet,flag,error_sent,var,comp):
    l = len(df)
    c = list(df.columns)
    temp_df = df#pd.DataFrame(columns = c)
    #print('master file update: error_sent=',error_sent,'var=',var,'comp=',comp)
    if(flag==1):
        for r in c:
            temp_df.loc[l+1,r]=" "
        temp_df.loc[l+1,'Sl. No. ']=l+1
        temp_df.loc[l+1,'Ref HSD'] = " "
        temp_df.loc[l+1,'Test Cycle ']=" "
        temp_df.loc[l+1,'Error count'] = " "
        temp_df.loc[l+1,'Ubuntu version ']=" "
        temp_df.loc[l+1,'Variables']=var
        temp_df.loc[l+1,'Comp Variables']=comp
        temp_df.loc[l+1,'Error string']=error_sent
        with pd.ExcelWriter(file_name) as writer:
            temp_df.to_excel(writer, sheet_name=sheet, index=False)
            df_dpmt.to_excel(writer, sheet_name="DPMT", index=False)
            df_dpmo_hsd.to_excel(writer, sheet_name="dpmo_hsd", index=False)
            df_dpmt_hsd.to_excel(writer, sheet_name="dpmt_hsd", index=False)      
        
    if(flag==2):
        for r in c:
            temp_df.loc[l+1,r]=" "
        temp_df.loc[l+1,'Sl. No. ']=l+1
        temp_df.loc[l+1,'Ref HSD'] = " "
        temp_df.loc[l+1,'Test Cycle ']=" "
        temp_df.loc[l+1,'Error count'] = " "
        temp_df.loc[l+1,'Ubuntu version ']=" "
        temp_df.loc[l+1,'Variables']=var
        temp_df.loc[l+1,'Comp Variables']=comp
        temp_df.loc[l+1,'Error string']=error_sent
        with pd.ExcelWriter(file_name) as writer:
            temp_df.to_excel(writer, sheet_name=sheet, index=False)
            df_dpmo.to_excel(writer, sheet_name="DPMO", index=False)
            df_dpmo_hsd.to_excel(writer, sheet_name="dpmo_hsd", index=False)
            df_dpmt_hsd.to_excel(writer, sheet_name="dpmt_hsd", index=False)



        
# Get the new error based on sentence matching using cosine similarity
def get_max_similarity(s1,error_string):
    score_flag = [0]*len(s1)
    score = []
    kk=0
    l_dpmo=len(df_dpmo)
    l_dpmt=len(df_dpmt)
    folder_update_get_max_sim = st.empty()
    u_cnt=1
    for x in s1:
        folder_update_get_max_sim.write("Checking for new (delta) error: "+str(u_cnt)+' of '+str(len(s1)))
        temp_score=[]
        temp_x=x
        #temp_x = "<url>new error acpi</url>"
        x=x.lower()
        X_set = set(word_tokenize(x))
        #for sent in error_string:
        for g in range(0,len(error_string)):
            sent=(error_string[g]).lower()
            #sent=sent.lower()
            Y_set = set(word_tokenize(sent))
            # form a set containing keywords of both strings
            l1 =[]
            l2 =[]
            rvector = X_set.union(Y_set)  
            for w in rvector: 
                if w in X_set: l1.append(1) # create a vector 
                else: l1.append(0) 
                if w in Y_set: l2.append(1) 
                else: l2.append(0) 
            c = 0              
            # cosine formula  
            for i in range(len(rvector)): 
                    c+= l1[i]*l2[i] 
            cosine = c / float((sum(l1)*sum(l2))**0.5)
            temp_score.append(cosine)
        max_score_index = np.argmax(temp_score)
        temp = temp_score[max_score_index]
        score.append(temp)
        if(temp <= SIMILARITY_SCORE):
            var=""
            comp_var=""
            if(max_score_index<l_dpmo):
                [var,comp_var]=get_additional_var(additional_variables,additional_comp_variables,temp_x)
                update_delta_error_in_master_file(df_dpmo,sheets[0],DPMO,temp_x,var,comp_var)
            if(max_score_index>=l_dpmo-1):
                [var,comp_var]=get_additional_var(additional_variables,additional_comp_variables,temp_x)
                update_delta_error_in_master_file(df_dpmt,sheets[1],DPMT,temp_x,var,comp_var)
            score_flag[kk]=1
        kk=kk+1
        u_cnt=u_cnt+1
    return(score_flag)    


def update_delta_error_file(df,file_name):
    c = list(df.columns)
    s = list(df[c[0]])
    score_flag = [0]*len(s)
    score = []
    k=0
    temp_df = pd.DataFrame(columns = ['variables','comp variables','Unique/Delta Errors'])
    u=[]
    for x in s:
        temp_score=[]
        temp_x=x
        x=x.lower()
        X_set = set(word_tokenize(x))        
        for sent in error_string:
            sent=sent.lower()
            Y_set = set(word_tokenize(sent))
            # form a set containing keywords of both strings
            l1 =[]
            l2 =[]
            rvector = X_set.union(Y_set)  
            for w in rvector: 
                if w in X_set: l1.append(1) # create a vector 
                else: l1.append(0) 
                if w in Y_set: l2.append(1) 
                else: l2.append(0) 
            c = 0              
            # cosine formula  
            for i in range(len(rvector)): 
                    c+= l1[i]*l2[i] 
            cosine = c / float((sum(l1)*sum(l2))**0.5)
            temp_score.append(cosine)
        temp = max(temp_score)
           
        if(temp <= SIMILARITY_SCORE):
            u.append(temp_x)
    for r in range(0,len(u)):
        temp_df.loc[r,'Unique/Delta Errors']=u[r]
        [v,c] = get_additional_var(additional_variables,additional_comp_variables,u[r])
        temp_df.loc[r,'variables']=v
        temp_df.loc[r,'comp variables']=c
    return(temp_df)

# Map the error statement with Master file and get variables and comp variables
def get_error_info_2_0_1(sent,error_string,variables,comp_variables):
    s  = list(sent)
    temp = []#difflib.get_close_matches(sent.lower(),error_string,n = 4,cutoff = 0.3)
    v = " "
    c = " "
    if(len(temp)>0):
        r = error_string.index(temp[0])
        v = variables[r]
        c = comp_variables[r]                
    return(v,c)

def get_error_info(sent,error_string,variables,comp_variables):
    v=''
    c=''
    for t in list(sent.split(" ")):
        if t in variables:
            v = t
        if t in comp_variables:
            c = t                        
    return(v,c)

def get_error_info2_0(sent,error_string,variables,comp_variables):
    temp = difflib.get_close_matches(sent.lower(),error_string,n = 4,cutoff = 0.3)
    v = " "
    c = " "
    if(len(temp)>0):
        r = error_string.index(temp[0])
        v = variables[r]
        c = comp_variables[r]                
    return(v,c)

def get_error_info_old(sent,error_string,variables,comp_variables):
    temp = difflib.get_close_matches(sent.lower(),error_string,n = 4,cutoff = 0.3)
    v = " "
    c = " "
    if(len(temp)>0):
        for r in range(0,len(error_string)):
            if(error_string[r]==temp[0]):
                v = variables[r]
                c = comp_variables[r]
                break 
    return(v,c)
    
# Get all sub-folders using recursion
def get_folders(dirname):
    subfolders = [f.path for f in os.scandir(dirname) if f.is_dir()]
    for dirname in list(subfolders):
        subfolders.extend(get_folders(dirname))
    return subfolders

#function to manage data frame with duplicate row to single row along with freq.
def manage_df(df):
    temp_df=df.groupby(df.columns.tolist(),dropna=False).size().reset_index()
    col = list(temp_df.columns)
    col[-1]='Frequency'
    temp_df.columns = col
    return(temp_df)

# Function to clean the text
def clean_text(text):
    end_index = text.find(']')
    if end_index != -1:
        return text[end_index + 2:] if len(text) > end_index + 2 and text[end_index + 1] == ' ' else text[end_index + 1:]
    return text

# Function to check if a line is a failure
def check_failure(line, failure_keywords, affirmative_keywords):
    line = clean_text(line.lower())
    line= (re.sub('\W+',' ', line)).split(" ")
    #print('cleaned line=',line)
    failure_present = any(keyword in line for keyword in failure_keywords)
    affirmative_present = any(keyword in line for keyword in affirmative_keywords)
    
    if failure_present and not affirmative_present:
        return True, next(keyword for keyword in failure_keywords if keyword in line)
    else:
        return False, None

    #('score flag = ',score_flag)
    return(score_flag)

def check_failure_old(line, failure_keywords, affirmative_keywords):
    line = clean_text(line)
    failure_present = any(keyword in line.lower() for keyword in failure_keywords)
    affirmative_present = any(keyword in line.lower() for keyword in affirmative_keywords)
    
    if failure_present and not affirmative_present:
        return True, next(keyword for keyword in failure_keywords if keyword in line.lower())
    else:
        return False, None

# Function to zip the output files
def compress(path,file_names):
    # Select the compression mode ZIP_DEFLATED for compression
    # or zipfile.ZIP_STORED to just store the file
    compression = zipfile.ZIP_DEFLATED
    # create the zip file first parameter path/name, second mode
    f = "output.zip"
    zf = zipfile.ZipFile(f, mode="w")
    try:
        for file_name in file_names:
            # Add file to the zip file
            # first parameter file to zip, second filename in zip
            zf.write(path + file_name, file_name, compress_type=compression)

    except FileNotFoundError:
        print("An error occurred")
    finally:
        # close the file!
        zf.close()
    return(f)


# Function to unzip all files in the folder
def unzip_files(folder_path):
    unzipped_paths = []
    for file_name in os.listdir(folder_path):
        if file_name.endswith('.zip'):
            file_path = os.path.join(folder_path, file_name)
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                extracted_path = folder_path
                zip_ref.extractall(extracted_path)
                unzipped_paths.append(extracted_path)
    return unzipped_paths

# Function to unzip all files in the folder
def unzip_files_old(folder_path):
    unzipped_paths = []
    for file_name in os.listdir(folder_path):
        if file_name.endswith('.zip'):
            file_path = os.path.join(folder_path, file_name)
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                extracted_path = os.path.join(folder_path, os.path.splitext(file_name)[0])
                zip_ref.extractall(extracted_path)
                unzipped_paths.append(extracted_path)
    return unzipped_paths

# Function to find 'dmesg' folder and process logs, adding parent directory for unique identification

def find_dmesg_folder_and_parse_logs(root_path, failure_keywords, affirmative_keywords,flag,error_string,variables,comp_variables):
    global log_lines_dmesg
    global log_lines_syslog
    global log_lines_dpmt
    global log_lines_total

    global log_files_dmesg
    global log_files_syslog
    global log_files_dpmt
    global log_files_total    
    data = []
    flag_all=1
    #if ((flag==FLAG_DPMO and 'dmesg' in root_path) or flag==FLAG_DPMT or 'syslog' in root_path):
    #if ((flag==FLAG_DPMO and 'dmesg' in root_path.lower()) or flag==FLAG_DPMT):
    #if ((flag==FLAG_DPMO and 'dpmo' in root_path.lower() and ('dmesg' in root_path.lower() or 'syslog' in root_path.lower())) or flag==FLAG_DPMT):
    #if ((flag==FLAG_DPMO and 'dpmo' in root_path.lower() and ('dmesg' in root_path.lower() or 'syslog' in root_path.lower())) or flag==FLAG_DPMT):
    if(flag_all == 1):
        dmesg_path = root_path
        parent_folder = os.path.basename(os.path.dirname(dmesg_path))  # Get parent folder for unique log identification        
        for log_file in os.listdir(dmesg_path):
            log_file_path = os.path.join(dmesg_path, log_file)
            if log_file.endswith('.log'):
                try:
                    #with open(log_file_path, 'r') as file:
                    #    lines = file.readlines()
                    with open(log_file_path,encoding="utf8") as file:
                        lines = file.readlines()
                        stats_df.loc[0,'Total Log Lines: '] = stats_df.loc[0,'Total Log Lines: ']+len(lines)

                except Exception as e:
                    if(STREAMLIT==1):
                        st.error(f"Error reading {log_file}: {e}")
                    else:
                        print(f"Error reading {log_file}: {e}")
                    continue
                #print('log_file_path =',log_file_path)
                if('dmesg' in root_path.lower()):
                    log_lines_dmesg=log_lines_dmesg+len(lines)
                    log_files_dmesg = log_files_dmesg+1
                if('syslog' in root_path.lower()):
                    log_lines_syslog=log_lines_syslog+len(lines)
                    log_files_syslog = log_files_syslog+1
                if('dpmt' in root_path.lower()):
                    log_lines_dpmt=log_lines_dpmt+len(lines)
                    log_files_dpmt = log_files_dpmt+1
#C:\Users\goelvikx\OneDrive - Intel Corporation\Desktop \unit_testing_10\DPMO\Coldboot_logs\ST10_Coldboot_rerun_Logs\coldboot_Logs_072424-181525\dmesg
#C:\Users\goelvikx\Downloads                   \Suneetha\GNR_RVP1       \DPMO\S4_Logs      \ST1_S4_logs             \S4_Logs_030425-175530      \dmesg
                if(VERSION==2):
                    logs_name_parts = os.path.splitext(log_file)[0].split('_')
                    print(logs_name_parts)#,'\tlogs_name_parts[-2]=',logs_name_parts[-2])
                    setup_id = logs_name_parts[-2] if len(logs_name_parts) > 1 else "not found"
                    test_cycle_id = logs_name_parts[-1] if len(logs_name_parts) > 1 else "not found"
                    print('setup_id=',setup_id)
                    print('test_cycle_id=',test_cycle_id)
                 
                for line in lines:
                    is_failure, key_issue = check_failure(line, failure_keywords, affirmative_keywords)
                    if is_failure:
                        stats_df.loc[0,'Error Log Lines: '] = stats_df.loc[0,'Error Log Lines: '] + 1
                        cleaned_text = clean_text(line.strip())
                        full_log_name = f"{parent_folder}/{log_file}"  # Prefix log with parent folder for unique naming
                        file_name=""
                        if(flag==FLAG_DPMT):
                            file_name = log_file_path.split(os.path.sep)[-3]
                        if(flag==FLAG_DPMO):
                            if("warmboot_logs" in log_file_path.lower()):
                                file_name = "Warmboot_Logs"
                            if("coldboot_logs" in log_file_path.lower()):
                                file_name = "Coldboot_Logs"
                            if("s1_logs" in log_file_path.lower()):
                                file_name = "S1_Logs"
                            if("s2_logs" in log_file_path.lower()):
                                file_name = "S2_Logs"                   
                            if("s3_logs" in log_file_path.lower()):
                                file_name = "S3_Logs"                  
                            if("s4_logs" in log_file_path.lower()):
                                file_name = "S4_Logs"                                          
                        var = "<>"
                        comp = "<>"
                        [var,comp] = get_error_info(cleaned_text,error_string,variables,comp_variables)
                        #print(cleaned_text,'\tvar=',var,'\tcomp=',comp)
                        if(VERSION==1):
                            data.append({'variables':var, 'comp variables':comp, 'text': cleaned_text, 'error type': key_issue, 'category': full_log_name,'DPMO/DPMT':category[flag],'DPMO Sub-Category':file_name,'SETUP_ID' : setup_id})                            
                        if(VERSION==2):
                            data.append({'variables':var, 'comp variables':comp, 'text': cleaned_text, 'error type': key_issue, 'category': full_log_name,'DPMO/DPMT':category[flag],'DPMO Sub-Category':file_name,'SETUP_ID' : setup_id, 'TEST_CYCLE_ID': test_cycle_id})                            

    return data



def find_dmesg_folder_and_parse_logs_from_server(host, username, password, root_path, failure_keywords, affirmative_keywords,flag,error_string,variables,comp_variables):
    global log_lines_dmesg
    global log_lines_syslog
    global log_lines_dpmt
    global log_lines_total

    global log_files_dmesg
    global log_files_syslog
    global log_files_dpmt
    global log_files_total    
    data = []
    flag_all=1
    #if ((flag==FLAG_DPMO and 'dmesg' in root_path) or flag==FLAG_DPMT or 'syslog' in root_path):
    #if ((flag==FLAG_DPMO and 'dmesg' in root_path.lower()) or flag==FLAG_DPMT):
    #if ((flag==FLAG_DPMO and 'dpmo' in root_path.lower() and ('dmesg' in root_path.lower() or 'syslog' in root_path.lower())) or flag==FLAG_DPMT):
    #if ((flag==FLAG_DPMO and 'dpmo' in root_path.lower() and ('dmesg' in root_path.lower() or 'syslog' in root_path.lower())) or flag==FLAG_DPMT):
    if(flag_all == 1):
        dmesg_path = root_path
        parent_folder = os.path.basename(os.path.dirname(dmesg_path))  # Get parent folder for unique log identification        
        for log_file in os.listdir(dmesg_path):
            log_file_path = os.path.join(dmesg_path, log_file)
            if log_file.endswith('.log'):
                try:
                    #with open(log_file_path, 'r') as file:
                    #    lines = file.readlines()
                    with open(log_file_path,encoding="utf8") as file:
                        lines = file.readlines()
                        stats_df.loc[0,'Total Log Lines: '] = stats_df.loc[0,'Total Log Lines: ']+len(lines)

                except Exception as e:
                    if(STREAMLIT==1):
                        st.error(f"Error reading {log_file}: {e}")
                    else:
                        print(f"Error reading {log_file}: {e}")
                    continue
                #print('log_file_path =',log_file_path)
                if('dmesg' in root_path.lower()):
                    log_lines_dmesg=log_lines_dmesg+len(lines)
                    log_files_dmesg = log_files_dmesg+1
                if('syslog' in root_path.lower()):
                    log_lines_syslog=log_lines_syslog+len(lines)
                    log_files_syslog = log_files_syslog+1
                if('dpmt' in root_path.lower()):
                    log_lines_dpmt=log_lines_dpmt+len(lines)
                    log_files_dpmt = log_files_dpmt+1
#C:\Users\goelvikx\OneDrive - Intel Corporation\Desktop \unit_testing_10\DPMO\Coldboot_logs\ST10_Coldboot_rerun_Logs\coldboot_Logs_072424-181525\dmesg
#C:\Users\goelvikx\Downloads                   \Suneetha\GNR_RVP1       \DPMO\S4_Logs      \ST1_S4_logs             \S4_Logs_030425-175530      \dmesg
                if(VERSION==2):
                    logs_name_parts = os.path.splitext(log_file)[0].split('_')
                    print(logs_name_parts)#,'\tlogs_name_parts[-2]=',logs_name_parts[-2])
                    setup_id = logs_name_parts[-2] if len(logs_name_parts) > 1 else "not found"
                    test_cycle_id = logs_name_parts[-1] if len(logs_name_parts) > 1 else "not found"
                    print('setup_id=',setup_id)
                    print('test_cycle_id=',test_cycle_id)
                 
                for line in lines:
                    is_failure, key_issue = check_failure(line, failure_keywords, affirmative_keywords)
                    if is_failure:
                        stats_df.loc[0,'Error Log Lines: '] = stats_df.loc[0,'Error Log Lines: '] + 1
                        cleaned_text = clean_text(line.strip())
                        full_log_name = f"{parent_folder}/{log_file}"  # Prefix log with parent folder for unique naming
                        file_name=""
                        if(flag==FLAG_DPMT):
                            file_name = log_file_path.split(os.path.sep)[-3]
                        if(flag==FLAG_DPMO):
                            if("warmboot_logs" in log_file_path.lower()):
                                file_name = "Warmboot_Logs"
                            if("coldboot_logs" in log_file_path.lower()):
                                file_name = "Coldboot_Logs"
                            if("s1_logs" in log_file_path.lower()):
                                file_name = "S1_Logs"
                            if("s2_logs" in log_file_path.lower()):
                                file_name = "S2_Logs"                   
                            if("s3_logs" in log_file_path.lower()):
                                file_name = "S3_Logs"                  
                            if("s4_logs" in log_file_path.lower()):
                                file_name = "S4_Logs"                                          
                        var = "<>"
                        comp = "<>"
                        [var,comp] = get_error_info(cleaned_text,error_string,variables,comp_variables)
                        #print(cleaned_text,'\tvar=',var,'\tcomp=',comp)
                        if(VERSION==1):
                            data.append({'variables':var, 'comp variables':comp, 'text': cleaned_text, 'error type': key_issue, 'category': full_log_name,'DPMO/DPMT':category[flag],'DPMO Sub-Category':file_name,'SETUP_ID' : setup_id})                            
                        if(VERSION==2):
                            data.append({'variables':var, 'comp variables':comp, 'text': cleaned_text, 'error type': key_issue, 'category': full_log_name,'DPMO/DPMT':category[flag],'DPMO Sub-Category':file_name,'SETUP_ID' : setup_id, 'TEST_CYCLE_ID': test_cycle_id})                            

    return data




# Function to save the DataFrame to a CSV in the required format
def save_to_csv(df, csv_file):
    grouped = df.groupby(['category', 'text']).size().reset_index(name='Frequency')
    grouped.columns = ['Log File Name', 'Unique Sentence', 'Frequency']
    grouped.to_csv(csv_file, index=False)

dpmo_list = ['\dpmo',"\dpmo", "dpmo/",'/dpmo/']
host = ''
username=''
password=''


def main(es,variables,comp_variables):
    global log_lines_dmesg
    global log_lines_syslog
    global log_lines_dpmt
    global log_lines_total
    
    global log_files_dmesg
    global log_files_syslog
    global log_files_dpmt
    global log_files_total      
    flag_file_write=0
    flag_folder_read=0
    temp_flag=1
    df_flag=0
    flag_server = 0
    global username
    global host
    global password

    # Streamlit App
    if(STREAMLIT==1):
        st.title("Linux Parser V2.1")
    else:
        print("Linux Parser V2.1")
    # File uploader to select the folder containing .log or .zip files
    if(STREAMLIT==1):
        local = 'Local M/C'
        remote = 'Remorte Server'
        option = st.radio('Select local or remote logs: ', [local,remote])
        if(option==local):
            flag_server=0
            folder_path = st.text_input('Enter the folder path containing .log or .zip files:')
        if(option==remote):
            flag_server=1
            host     = st.text_input('Host ID  : ')
            username = st.text_input('username : ')
            password = st.text_input('Password : ')
            #password = st.text_input("Enter Password:", type="password")
            folder_path = st.text_input('Enter the folder path containing .log or .zip files:')

#            print('user name=',username)
#            print('host=',host)
#            print('pssword=',password)
#            print('folder path = ',folder_path)
        
#        folder_path = st.text_input('Enter the folder path containing .log or .zip files:')
    else:
        #folder_path="C://Users//goelvikx//Downloads//unit_testing_4"
        folder_path = "C://Users//goelvikx//OneDrive - Intel Corporation//Desktop//unit_testing_4"
    if folder_path:
        if os.path.exists(folder_path):
            if(flag_folder_read==0):
                flag_folder_read=1
                if(STREAMLIT==1):
                    st.write(f"Processing files in folder: {folder_path}")
                    folder_update_name = st.empty()
                    msg_update_box_dmesg = st.empty()
                    msg_update_box_syslog = st.empty()
                    msg_update_box_dpmt = st.empty()
                    msg_update_box_total = st.empty()
                    msg_update_box = st.empty()
                else:
                    print(f"Processing files in folder: {folder_path}")
                if(temp_flag==1):
                    start = time.time()
                    stats_df.loc[0,'Start Time: ']=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    if(flag_server==0):
                        # Unzip all files
                        folders = get_folders(folder_path)
                        unzipped_paths = unzip_files(folder_path)
                        folders = get_folders(folder_path)
                        z_cnt=1
                        unzipped_paths=[]
                        l = len(folders)
                        for r in folders:
                            if('dpmo' in r.lower() or 'dpmt' in r.lower()):
                                if(STREAMLIT==1):
                                    folder_update_name.write("Unzipping folder = "+r)
                                    msg_update_box.write("Unzipping " + str(z_cnt) + " of " + str(l))
                                unzipped_paths.append(unzip_files(r))
                            z_cnt=z_cnt+1
                        #print('unzipped_paths=',unzipped_paths)
                        folders = get_folders(folder_path)
                    if(flag_server==1):
                        [channel,client] = remote_connection(host, username, password)
                        folders = get_folder_from_server(channel,client,folder_path)

                    
                    stats_df.loc[0,'Total Folders: '] = len(folders)
                    # Parse the unzipped folders and log files
                    all_data = []
                    r_cnt=1
                    dpmt_folders=0
                    dpmo_folders=0
                    dmesg_folders=0
                    syslog_folders=0
                    other_folders=0
                    for r in folders:
                        flag=0
                        k = "\tParsing Folders: " + str(r_cnt) + " of " + str(len(folders))
                        if('dpmo' in r.lower()):
                            flag = FLAG_DPMO
                            dpmo_folders=dpmo_folders+1
                        if('dmesg' in r.lower()):
                            dmesg_folders=dmesg_folders+1
                        if('syslog' in r.lower()):
                            syslog_folders=syslog_folders+1    
                        if('dpmt' in r.lower()):
                            flag = FLAG_DPMT
                            dpmt_folders=dpmt_folders+1
                        if(flag==0):
                            other_folders=other_folders+1
                            
                        if(STREAMLIT==1):
                            folder_update_name.write("folder = "+r)
                            msg_update_box.write(k)
                        if(STREAMLIT==0):
                            print(k)
                        if(flag>=1):
                            all_data.extend(find_dmesg_folder_and_parse_logs(r, failure_keywords, affirmative_keywords,flag,es,variables,comp_variables))
                        r_cnt=r_cnt+1
                    stats_df.loc[0,'DPMO Folders: '] = dpmo_folders
                    stats_df.loc[0,'DPMT Folders: '] = dpmt_folders
                    stats_df.loc[0,'DMESG Folders: '] = dmesg_folders
                    stats_df.loc[0,'SYSLOG Folders: '] = syslog_folders
                    
                    stats_df.loc[0,'Other Folders: '] = other_folders
                    log_lines_total = log_lines_dmesg + log_lines_syslog + log_lines_dpmt
                    log_files_total = log_files_dmesg + log_files_syslog + log_files_dpmt
                    
                    if(STREAMLIT==1):
                        msg_update_box_dmesg.write ('dmesg  log lines = ' + str(log_lines_dmesg) + '\tLog Files = '+str(log_files_dmesg))
                        msg_update_box_syslog.write('syslog log lines = ' + str(log_lines_syslog) + '\tLog Files = '+str(log_files_syslog))
                        msg_update_box_dpmt.write  ('dpmt   log lines = ' + str(log_lines_dpmt) + '\tLog Files = '+str(log_files_dpmt))
                        msg_update_box_total.write ('total  log lines = ' + str(log_lines_total) + '\tLog Files = '+str(log_files_total))
                    else:
                        print('dmesg  log lines = ' + str(log_lines_dmesg) + '\tLog Files = '+str(log_files_dmesg))
                        print('syslog log lines = ' + str(log_lines_syslog) + '\tLog Files = '+str(log_files_syslog))
                        print('dpmt   log lines = ' + str(log_lines_dpmt) + '\tLog Files = '+str(log_files_dpmt))
                        print('total  log lines = ' + str(log_lines_total) + '\tLog Files = '+str(log_files_total))
                        
                    # Convert to DataFrame
                    df = pd.DataFrame(all_data)
                    
                    if not df.empty:
                        df_flag=1
                        # Second Frequency Analysis: Consolidated frequency of each unique sentence across all files
                        st.subheader("Consolidated Frequency Analysis of Unique Sentences")
                        consolidated_freq_df = df.groupby(['text','variables','comp variables'],dropna=False).size().reset_index()
                        consolidated_freq_df.columns = ['Unique Sentence', 'variables', 'comp variables', 'Frequency']
                    
                        if(STREAMLIT==1):
                            st.subheader("Parsed Data")
                            st.dataframe(df.head(MAX_DISP), height=200)
                        # First Frequency Analysis: Frequency of each category (file-specific)
                        if(STREAMLIT==1):
                            st.subheader("Frequency Analysis by Category and Log File")
                        frequency_df = df['category'].value_counts().reset_index()
                        frequency_df.columns = ['Category', 'Frequency']
                        # Dropdown to display unique sentences per category (with file name included)

                        selected_category=''
                        if(STREAMLIT==1):
                            selected_category = st.selectbox("Select a log file to view details:", frequency_df['Category'])
                            
                        if (STREAMLIT==1 and selected_category):
                            unique_sentences_df = df[df['category'] == selected_category]['text'].value_counts().reset_index()
                            unique_sentences_df.columns = ['Unique Sentence', 'Count']
                            st.write(f"Unique sentences for log: {selected_category}")
                            #st.table(unique_sentences_df)
                            st.dataframe(unique_sentences_df.head(MAX_DISP), height=200)
                        # Second Frequency Analysis: Consolidated frequency of each unique sentence across all files
                        if(STREAMLIT==1):
                            st.subheader("Consolidated Frequency Analysis of Unique Sentences")
                            #st.table(consolidated_freq_df)
                            st.dataframe(consolidated_freq_df.head(MAX_DISP), height=200)
                    temp_flag=0

                    # Save to CSV
                    flag_save=1
                    #if((STREAMLIT==0 or st.button("Save to CSV"))):
                    if(flag_save and  not df.empty):
                        if(flag_server==0):
                            filename = os.path.basename(folder_path).split('/')[-1]                            
                            csv_file = folder_path + '//' + filename + '_failures.xlsx'                             
                            temp_csv_file_ = folder_path + '//' + filename + '_temp_failures.xlsx'
                            csv_file_master = folder_path + '_delta_errors.csv'
                        if(flag_server==1):
                            [channel,client] = remote_connection(host, username, password)
                            temp_path=os.getcwd()
                            print('temp_path=',temp_path)
                            filename = folder_path.split('/')[-1]
                            csv_file = temp_path +'/' + filename + '_failures.xlsx'                    
                            csv_file_master = temp_path +'/'+folder_path + '_delta_errors.csv'
                            
                        s1 = list(consolidated_freq_df['Unique Sentence'])

                        final_df = manage_df(df)
                        [csv_files,start_indexes,end_indexes]=get_file_names(folder_path + '//' + filename + '_failures',len(final_df))
                       
                        for file_index in range(0,len(csv_files)):
                            st.subheader("Saving output file: " + str(file_index+1) + "of " + str(len(csv_files)))
                        #get_max_similarity(s1,es)
                            with pd.ExcelWriter(csv_files[file_index]) as writer:
                                #print(csv_files[file_index],writer)
                                temp_df = consolidated_freq_df.drop('Frequency', axis=1)
                                col = list(temp_df.columns)
                                col[0]= 'Unique/Delta Errors'
                                temp_df.columns = col
                               
                                temp_df = pd.DataFrame()#update_delta_error_file(temp_df,'')
                                temp_df.to_excel(writer, sheet_name="unique_delta_errors", index=True)
                                flag_delta_error=0
                                if(len(temp_df)>0):
                                    flag_delta_error=1
                                    project_path = os.getcwd()
                                    file_name = project_path + "//DPMT_DPMO_failure_error_listing.xlsx"

                                    df_dpmt = pd.read_excel(file_name, sheet_name='DPMT')
                                    df_dpmo = pd.read_excel(file_name, sheet_name='DPMO')
                                    df_dpmt_hsd = pd.read_excel(file_name, sheet_name='dpmt_hsd')
                                    df_dpmo_hsd = pd.read_excel(file_name, sheet_name='dpmo_hsd')

                                    df_dpmo_mt = pd.concat([df_dpmo, df_dpmt], axis=0)

                                    error_string   = list(df_dpmo_mt['Error string'].str.lower())
                                    variables      = list(df_dpmo_mt['Variables'])
                                    comp_variables = list(df_dpmo_mt['Comp Variables'])
                                    es=error_string
                                    
                                # Save the log file-specific unique sentence frequency analysis to CSV
                                final_df = manage_df(df)
                                temp_df = final_df.drop('error type',axis=1)
                                tdf = temp_df.iloc[start_indexes[file_index]:end_indexes[file_index]]
                                tdf.to_excel(writer, sheet_name="Failures", index=True)

                                if(file_index==0):    
                                    c_df = pd.DataFrame(columns= ['variables','comp variables','Unique Sentence','SETUP_ID','Frequency'])
                                    c_df['Unique Sentence'] = consolidated_freq_df['Unique Sentence']
                                    c_df['Frequency'] = consolidated_freq_df['Frequency']
                                    c_df['variables'] = consolidated_freq_df['variables']
                                    c_df['comp variables'] = consolidated_freq_df['comp variables']
                                    
                                    c_df.groupby(['Unique Sentence'])['SETUP_ID'].apply(lambda grp: list(set(grp))).reset_index()    
                                    tdf = df.groupby(['text'])['SETUP_ID'].apply(lambda grp: list(set(grp))).reset_index()
                                    c_df['SETUP_ID'] = tdf['SETUP_ID']
                                    r_cnt=1
                                    if(STREAMLIT==1):
                                        msg_update_box = st.empty()
                                    end = time.time()
                                    stats_df.loc[0,'End Time: ']=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                                    lt= end-start
                                    lt_hour = lt//3600
                                    lt_min  = (lt%3600)//60
                                    lt_sec = int((lt%3600)%60)
                                    stats_df.loc[0,'Lapsed Time: ']='Hours: ' + str(lt_hour) + ', Minutes: ' + str(lt_min) + ', Seconds: '+str(lt_sec)

                                c_df.to_excel(writer, sheet_name="unique_error_frequency", index=True)
                                #stats_df=stats_df.transpose()
                                stats_df.loc[0,'Percent Errors: '] = str(round((stats_df.loc[0,'Error Log Lines: ']*100)/stats_df.loc[0,'Total Log Lines: '],2))+"%"
                                stats_df_temp=stats_df.drop(['DPMO Folders: ','DPMT Folders: ','DMESG Folders: ','SYSLOG Folders: ','Other Folders: '], axis=1)                                
                                (stats_df_temp.transpose()).to_excel(writer, sheet_name="Stats", index=True)
                                                    
                        if(STREAMLIT==1 and flag_server==0):
                            st.success(f"Data saved to '{csv_file}'")
                            if(len(csv_files)==1):
                                with open(csv_files[0], 'rb') as f:
                                    st.download_button(
                                        label='📥 Download Output File',
                                        data=f ,
                                        file_name= csv_file,
                                        mime="application/vnd.ms-excel"
                                        )
                            else:
                                file_names = []#[os.path.basename(csv_file).split('/')[-1], os.path.basename(csv_file_master).split('/')[-1]]
                                for f in csv_files:
                                    file_names.append(os.path.basename(f).split('/')[-1])
                                path = folder_path+"//"
    
                                z=compress(path, file_names)

                                # Provide a link to download the zip file
                                with open(z, 'rb') as f:
                                    st.download_button(
                                        label="Download output.zip",
                                        data=f,
                                        file_name=z,
                                        mime='application/zip'
                                        )
                        else:
                            print(f"Data saved to '{csv_file}'")                   


                        if(STREAMLIT==1 and flag_server==1):
                            file_name = os.path.basename(csv_file )  #eds_report.csv
                            file_path = os.path.dirname(csv_file )
                            server_file = folder_path+"//"+file_name
                            ftp_client= client.open_sftp()
                            filename = os.path.basename(csv_file).split('/')[-1]
                            #print('file_name=',file_name)
                            #print('file_path=',file_path)
                            ftp_client.put(csv_file,folder_path+"//"+file_name)
                            st.success(f"Data saved to remore server path at '{server_file}'")


                    if (df_flag==0):
                        if(STREAMLIT==1):
                            st.write("No failures found in the provided log files.")
                        else:
                            print(("No failures found in the provided log files."))

            else:
                if(STREAMLIT==1):
                    st.write("No failures found in the provided log files.")
                else:
                    print(("No failures found in the provided log files."))
        else:
            if(STREAMLIT==1):
                st.error("The provided folder path does not exist.")
            else:
                print("The provided folder path does not exist.")
    else:
        flag_folder_read=0
        if(STREAMLIT==1):
            st.write("Please enter a folder path to start parsing.")
        else:
            print("Please enter a folder path to start parsing.")    

main(error_string,variables,comp_variables)

