
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

STREAMLIT=1

project_path =  os.getcwd()
SIMILARITY_SCORE = 0.90

DPMO = 1
DPMT = 2

VERSION = 2

category=['','DPMO','DPMT']
sheets=['DPMO','DPMT']

# Define the failure and affirmative keywords
failure_keywords =  [
                        "fail", "MissingConfigError", "TimeoutError", "UnknownPluginError", "No such", 
                        "ERROR", "AttributeError", "ModuleNotFoundError", "Assertion error", "TypeError", 
                        "ValueError", "NameError", "RuntimeError", "Valueerror", "EmptyTable", "IPC_Error", 
                        "not found", "error", "failed", "critical","EmptyTable","IndexError", "not"
                    ]

additional_variables=[
                        "ts", "info", "comp", "pci_id", "msg", 
                        "err", "error", "warning", "acpi_id", "notice", 
                        "debug", "command", "code", "event", "commit", "url", 
                        "version"
                    ]

additional_comp_variables=[
                        'acpi','alarm','aspm','audio','boot','bt','camera','charger','cnvi','codec','coreboot','cpu','cr50','cse','d3','dc6','dma','dmic',
                        'dock','ec','firmware','fsp','gfx','gpio','hdmi','i2c','lid','logs/kernel','lpss','media','mei','memory','nvme','pc10','pch','pci',
                        'pcie','perf','pg','pmc','power','retimer','runtimepm','s0ix','s3','sd','sensor','slp_s0','soundwire','spi','ssd','tbt','thermal'
                        'touch','tpm','usb','vbt','wifi','wwan'
                    ]

low_prirority_words = set(['error','err','fail'])
def get_additional_var(additional_variables,additional_comp_variables,s):
    var=""
    comp_var=""
    v=[]
    for x in additional_variables:
        if x in s:
            v.append(x)
    if(len(v)>0):
        ss=[]
        if(len(v)==1):
            ss = v[0]
        if(len(v)>1):
            ss = list(sorted(set(v) - low_prirority_words))
        if(len(ss)==0):
           var = v[0] 
        if(len(ss)>=1):
            var = ss[0]
        #t=additional_variables.index(var)
    c=[]
    for r in additional_comp_variables:
        if(r in s):
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
variables      = list(df_dpmo_mt['Variables'])
comp_variables = list(df_dpmo_mt['Comp Variables'])

append_df = pd.DataFrame(columns = df_dpmo_mt.columns)

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
    k=0
    l_dpmo=len(df_dpmo)
    l_dpmt=len(df_dpmt)
    for x in s1:
        temp_score=[]
        temp_x=x
        #temp_x = "<url>new error acpi</url>"
        x=x.lower()
        X_set = set(word_tokenize(x))
        #for sent in error_string:
        for g in range(0,len(error_string)):
            sent=error_string[g]
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
        max_score_index = np.argmax(temp_score)
        temp = temp_score[max_score_index]#max(temp_score)
        score.append(temp)
        if(temp <= SIMILARITY_SCORE):
            var=""
            comp_var=""
            if(max_score_index<l_dpmo):
                [var,comp_var]=get_additional_var(additional_variables,additional_comp_variables,temp_x)
                #print('temp_x=',temp_x,'dpmo var=',var,'comp=',comp_var)
                update_delta_error_in_master_file(df_dpmo,sheets[0],DPMO,temp_x,var,comp_var)
            if(max_score_index>=l_dpmo-1):
                [var,comp_var]=get_additional_var(additional_variables,additional_comp_variables,temp_x)
                #print('temp_x=',temp_x,'dpmt var comp=',var,comp_var)
                update_delta_error_in_master_file(df_dpmt,sheets[1],DPMT,temp_x,var,comp_var)
            score_flag[k]=1
        k=k+1
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
    #print('temp_df=',temp_df)    
    #temp_df.to_csv(file_name,index=False)
    return(temp_df)

# Map the error statement with Master file and get variables and comp variables
def get_error_info(sent,error_string,variables,comp_variables):
    temp = difflib.get_close_matches(sent.lower(),error_string,n = 4,cutoff = 0.3)
    v = " "
    c = " "
    #print('sent=',sent)
    #print('temp=',temp)
    if(len(temp)>0):
        for r in range(0,len(error_string)):
            if(error_string[r]==temp[0]):
                v = variables[r]
                c = comp_variables[r]
                break 
    #print('var=',v,'comp=',c)
    return(v,c)
    
# dict. key-value pair --> {key: ['variables','comp variables']}
var_comp={
    "fail":['',''],
    "missingconfigerror"  : ['<info>' , 'power'],
    "timeouterror"        : ['<error>', 'pci'],
    "unknownpluginerror"  : ['<error>', 'pci'],
    "no such"             : ['<info>' , 'logs/kernel'], 
    "ERROR"               : ['<ERROR>', 'ERROR'],
    "attributeerror"      : ['<error>', 'gfx'],
    "modulenotfounderror" : ['<info>' , 'power'],
    "assertion error"     : ['<error>', 'pci'],
    "typeerror"           : ['<info>' , 'power'], 
    "valueerror"          : ['<info>' , 'power'],
    "nameerror"           : ['<info>' , 'power'],
    "runtimeerror"        : ['<error>', 'power'],
    "valueerror"          : ['<error>', 'power'],
    "emptytable"          : [''       , ''],
    "ipc_error"           : ['<info>' , 'cpu'], 
    "not found"           : [''       , ''],
    "error"               : ['<error>', 'error'],
    "failed"              : [''       , ''],
    "critical"            : [''       , ''],
    "emptytab;e"          : ['<info>' , 'logs/kernel'],
    "indexerror"          : ['<info>' , 'power']
}

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
            try:
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    extracted_path = os.path.join(folder_path, os.path.splitext(file_name)[0])
                    zip_ref.extractall(extracted_path)
                    unzipped_paths.append(extracted_path)
            except zipfile.BadZipFile:
                if(STREAMLIT==1):
                    st.error(f"Error unzipping file: {file_name}")
                else:
                    print(f"Error unzipping file: {file_name}")
    return unzipped_paths

# Function to find 'dmesg' folder and process logs, adding parent directory for unique identification
# Function to find 'dmesg' folder and process logs, adding parent directory for unique identification
def find_dmesg_folder_and_parse_logs(root_path, failure_keywords, affirmative_keywords,flag,error_string,variables,comp_variables):
    data = []
    if 'dmesg' in root_path  and 'results' not in root_path:
        dmesg_path = root_path#os.path.join(root_path, 'dmesg')
        parent_folder = os.path.basename(os.path.dirname(dmesg_path))  # Get parent folder for unique log identification        
        for log_file in os.listdir(dmesg_path):
            log_file_path = os.path.join(dmesg_path, log_file)
            if log_file.endswith('.log'):
                try:
                    with open(log_file_path, 'r') as file:
                        lines = file.readlines()
                except Exception as e:
                    if(STREAMLIT==1):
                        st.error(f"Error reading {log_file}: {e}")
                    else:
                        print(f"Error reading {log_file}: {e}")
                    continue
           
                if(VERSION==2):
                    logs_name_parts = os.path.splitext(log_file)[0].split('_')
                    setup_id = logs_name_parts[-2] if len(logs_name_parts) > 1 else "not found"
                    test_cycle_id = logs_name_parts[-1] if len(logs_name_parts) > 1 else "not found"
 
                for line in lines:
                    is_failure, key_issue = check_failure(line, failure_keywords, affirmative_keywords)
                    if is_failure:
                        cleaned_text = clean_text(line.strip())
                        
                        #full_log_name = log_file_path#f"{parent_folder}/{log_file}"  # Prefix log with parent folder for unique naming
                        full_log_name = f"{parent_folder}/{log_file}"  # Prefix log with parent folder for unique naming
                        file_name=""
                        if(flag==1):
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
                        [var,comp] = get_error_info(cleaned_text,error_string,variables,comp_variables)

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

dpmt_list = ['\dpmt',"\dpmt", "dpmt/",'/dpmt/']
dpmo_list = ['\dpmo',"\dpmo", "dpmo/",'/dpmo/']

def main(es,variables,comp_variables):
    flag_file_write=0
    flag_folder_read=0

    # Streamlit App
    if(STREAMLIT==1):
        st.title("Linux Parser V1.1")
    else:
        print("Linux Parser V1.1")
    # File uploader to select the folder containing .log or .zip files
    if(STREAMLIT==1):
        folder_path = st.text_input('Enter the folder path containing .log or .zip files:')
    else:
        folder_path="C://Users//goelvikx//Downloads//unit_testing"
        #folder_path = "C://Users//goelvikx//OneDrive - Intel Corporation//Desktop//unit_testing_1"
    if folder_path:
        if os.path.exists(folder_path):
            if(flag_folder_read==0):
                flag_folder_read=1
                if(STREAMLIT==1):
                    st.write(f"Processing files in folder: {folder_path}")
                else:
                    print(f"Processing files in folder: {folder_path}")
                # Unzip all files
                unzipped_paths = unzip_files(folder_path)

                folders = get_folders(folder_path)

                unzipped_paths = unzip_files(folder_path)
                for r in folders:
                    unzipped_paths.append(unzip_files(r))
           
                folders = get_folders(folder_path)
        
                # Parse the unzipped folders and log files
                all_data = []
                for r in folders:
                    flag=0
                    if (any(ele in r.lower() for ele in dpmo_list)):
                        flag=1
                    if (any(ele in r.lower() for ele in dpmt_list)):
                        flag=2
                    #sheet_name=sheets[flag]
                    #print(r,'flag=',flag)
                    all_data.extend(find_dmesg_folder_and_parse_logs(r, failure_keywords, affirmative_keywords,flag,es,variables,comp_variables))

                # Convert to DataFrame
                
                df = pd.DataFrame(all_data)#, columns=['variables','comp variables','text', 'error type', 'category','DPMO/DPMT','SETUP_ID','TEST_CYCLE_ID'])

                # Second Frequency Analysis: Consolidated frequency of each unique sentence across all files
                #st.subheader("Consolidated Frequency Analysis of Unique Sentences")
                consolidated_freq_df = df['text'].value_counts().reset_index()
                consolidated_freq_df.columns = ['Unique Sentence', 'Frequency']
                #st.table(consolidated_freq_df)

            if not df.empty:
                if(STREAMLIT==1):
                    st.subheader("Parsed Data")
                    st.dataframe(df, height=200)

                # First Frequency Analysis: Frequency of each category (file-specific)
                if(STREAMLIT==1):
                    st.subheader("Frequency Analysis by Category and Log File")
                frequency_df = df['category'].value_counts().reset_index()
                frequency_df.columns = ['Category', 'Frequency']

                # Dropdown to display unique sentences per category (with file name included)
                if(STREAMLIT==1):
                    selected_category = st.selectbox("Select a log file to view details:", frequency_df['Category'])

                if (STREAMLIT==1 and selected_category):
                    unique_sentences_df = df[df['category'] == selected_category]['text'].value_counts().reset_index()
                    unique_sentences_df.columns = ['Unique Sentence', 'Count']
                    st.write(f"Unique sentences for log: {selected_category}")
                    st.table(unique_sentences_df)

                # Second Frequency Analysis: Consolidated frequency of each unique sentence across all files
                if(STREAMLIT==1):
                    st.subheader("Consolidated Frequency Analysis of Unique Sentences")
                    st.table(consolidated_freq_df)

                # Save to CSV
                if (STREAMLIT==0 or st.button("Save to CSV")):
                    filename = os.path.basename(folder_path).split('/')[-1]
                    csv_file = folder_path + '//' + filename + '_failures.xlsx'                    
                    csv_file_master = folder_path + '_delta_errors.csv'

                    s1 = list(consolidated_freq_df['Unique Sentence'])
                    #print('error_string=',es)
                    get_max_similarity(s1,es)

                    
                    with pd.ExcelWriter(csv_file) as writer:
                        temp_df = consolidated_freq_df.drop('Frequency', axis=1)
                        col = list(temp_df.columns)
                        col[0]= 'Unique/Delta Errors'
                        temp_df.columns = col
                        temp_df = update_delta_error_file(temp_df,'')
                        temp_df.to_excel(writer, sheet_name="unique_delta_errors", index=False)
                        #print('len temp_df=',len(temp_df))
                        if(len(temp_df)>0):
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
                        #final_df.to_csv(csv_file ,index = False) #atul

                        s = list(temp_df['text'])
                        for u in range(0,len(s)):
                            [var,comp] = get_error_info(s[u],es,variables,comp_variables)
                            temp_df.loc[u,'variables']=var
                            temp_df.loc[u,'comp variables']=comp 
                        temp_df.to_excel(writer, sheet_name="Failures", index=False)
                        
                        c_df = pd.DataFrame(columns= ['variables','comp variables','Unique Sentence','SETUP_ID','Frequency'])
                        c_df['Unique Sentence'] = consolidated_freq_df['Unique Sentence']
                        c_df['Frequency'] = consolidated_freq_df['Frequency']
                        s = list(consolidated_freq_df['Unique Sentence'])
                        for u in range(0,len(s)):
                            [var,comp] = get_error_info(s[u],es,variables,comp_variables)
                            c_df.loc[u,'variables']=var
                            c_df.loc[u,'comp variables']=comp        

                        c_df.groupby(['Unique Sentence'])['SETUP_ID'].apply(lambda grp: list(set(grp))).reset_index()    
                        tdf = df.groupby(['text'])['SETUP_ID'].apply(lambda grp: list(set(grp))).reset_index()
                        t = list(tdf['text'])
                        c = list(c_df['Unique Sentence'])
                        for u in range(0,len(c)):
                            for v in range(0,len(c)):
                                if(c[u]==t[v]):
                                    #l=tdf.loc[v,'SETUP_ID']
                                    s=""
                                    for r in  tdf.loc[v,'SETUP_ID']:
                                        s=s+r+", "
                                    res = '' 
                                    for i in range(len(s)-2):
                                        res += s[i]
                                    s=res
                                    c_df.loc[u,'SETUP_ID']=s
                                    break
                        c_df.to_excel(writer, sheet_name="unique_error_frequency", index=False)
                                           
                    if(STREAMLIT==1):
                        st.success(f"Data saved to '{csv_file}'")
                        with open(csv_file, 'rb') as f:
                            st.download_button(
                                label='📥 Download Output File',
                                data=f ,
                                file_name= csv_file,
                                mime="application/vnd.ms-excel"
                                )                        
                    else:
                        print(f"Data saved to '{csv_file}'")
                    

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
