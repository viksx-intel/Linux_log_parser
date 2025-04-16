import streamlit as st
import pandas as pd
import os
import zipfile

# Define the failure and affirmative keywords
failure_keywords = [
    "fail", "MissingConfigError", "TimeoutError", "UnknownPluginError", "No such", 
    "ERROR", "AttributeError", "ModuleNotFoundError", "Assertion error", "TypeError", 
    "ValueError", "NameError", "RuntimeError", "Valueerror", "EmptyTable", "IPC_Error", 
    "not found", "error", "failed", "critical","EmptyTable","IndexError"
]
affirmative_keywords = ["success", "successful", "completed"]

var_comp={
    "fail":['',''],
    "missingconfigerror":['<info>','power'],
    "timeouterror":['<error>','pci'],
    "unknownpluginerror":['<error>','pci'],
    "no such":['<info>','logs/kernel'], 
    "ERROR":['<ERROR>','ERROR'],
    "attributeerror":['<error>','gfx'],
    "modulenotfounderror":['<info>','power'],
    "assertion error":['<error>','pci'],
    "typeerror":['<info>','power'], 
    "valueerror":['<info>','power'],
    "nameerror":['<info>','power'],
    "runtimeerror":['<error>','power'],
    "valueerror":['<error>','power'],
    "emptytable":['',''],
    "ipc_error":['<info>','cpu'], 
    "not found":['',''],
    "error":['<error>','error'],
    "failed":['',''],
    "critical":['',''],
    "emptytab;e":['<info>','logs/kernel'],
    "indexerror":['<info>','power']
}

# Get all sub-folders using recursion
def get_folders(dirname):
    subfolders = [f.path for f in os.scandir(dirname) if f.is_dir()]
    for dirname in list(subfolders):
        subfolders.extend(get_folders(dirname))
    return subfolders

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
                st.error(f"Error unzipping file: {file_name}")
    return unzipped_paths

# Function to find 'dmesg' folder and process logs, adding parent directory for unique identification
def find_dmesg_folder_and_parse_logs(root_path, failure_keywords, affirmative_keywords):
    data = []
    
    for dirpath, dirnames, filenames in os.walk(root_path):
        if 'dmesg' in dirnames:
            dmesg_path = os.path.join(dirpath, 'dmesg')
            parent_folder = os.path.basename(os.path.dirname(dmesg_path))  # Get parent folder for unique log identification
            
            for log_file in os.listdir(dmesg_path):
                log_file_path = os.path.join(dmesg_path, log_file)
                if log_file.endswith('.log'):
                    try:
                        with open(log_file_path, 'r') as file:
                            lines = file.readlines()
                    except Exception as e:
                        st.error(f"Error reading {log_file}: {e}")
                        continue

                    for line in lines:
                        is_failure, key_issue = check_failure(line, failure_keywords, affirmative_keywords)
                        if is_failure:
                            cleaned_text = clean_text(line.strip())
                            full_log_name = f"{parent_folder}/{log_file}"  # Prefix log with parent folder for unique naming
                            #data.append({'text': cleaned_text, 'error type': key_issue, 'category': full_log_name, 'variables':key_issue,'comp variables':key_issue})
                            #data.append({'text': cleaned_text, 'error type': key_issue, 'category': full_log_name, 'variables':var_comp[key_issue.lower()][0], 'comp variables':var_comp[key_issue.lower()][1]})
                            data.append({'variables':var_comp[key_issue.lower()][0], 'comp variables':var_comp[key_issue.lower()][1], 'text': cleaned_text, 'error type': key_issue, 'category': full_log_name})
            break  # Stop once the 'dmesg' folder is processed
    return data

# Function to save the DataFrame to a CSV in the required format
def save_to_csv(df, csv_file):
    grouped = df.groupby(['category', 'text']).size().reset_index(name='Frequency')
    grouped.columns = ['Log File Name', 'Unique Sentence', 'Frequency']
    grouped.to_csv(csv_file, index=False)

# Streamlit App
st.title("Linux Parser v1.0")

# File uploader to select the folder containing .log or .zip files
folder_path = st.text_input('Enter the folder path containing .log or .zip files:')

if folder_path:
    if os.path.exists(folder_path):
        st.write(f"Processing files in folder: {folder_path}")
        
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
            all_data.extend(find_dmesg_folder_and_parse_logs(r, failure_keywords, affirmative_keywords))

        # Convert to DataFrame
        df = pd.DataFrame(all_data, columns=['variables','comp variables','text', 'error type', 'category',])

        if not df.empty:
            st.subheader("Parsed Data")
            st.dataframe(df, height=200)

            # First Frequency Analysis: Frequency of each category (file-specific)
            st.subheader("Frequency Analysis by Category and Log File")
            frequency_df = df['category'].value_counts().reset_index()
            frequency_df.columns = ['Category', 'Frequency']

            # Dropdown to display unique sentences per category (with file name included)
            selected_category = st.selectbox("Select a log file to view details:", frequency_df['Category'])

            if selected_category:
                unique_sentences_df = df[df['category'] == selected_category]['text'].value_counts().reset_index()
                unique_sentences_df.columns = ['Unique Sentence', 'Count']
                st.write(f"Unique sentences for log: {selected_category}")
                st.table(unique_sentences_df)

            # Second Frequency Analysis: Consolidated frequency of each unique sentence across all files
            st.subheader("Consolidated Frequency Analysis of Unique Sentences")
            consolidated_freq_df = df['text'].value_counts().reset_index()
            consolidated_freq_df.columns = ['Unique Sentence', 'Frequency']
            st.table(consolidated_freq_df)

            # Save to CSV
            if st.button("Save to CSV"):
                csv_file = folder_path+'_failures.csv'

                # Save the log file-specific unique sentence frequency analysis to CSV
                df.to_csv(csv_file,index=False)
                #save_to_csv(df, csv_file)
                
                st.success(f"Data saved to '{csv_file}'")

                # Provide a link to download the CSV
                with open(csv_file, 'rb') as f:
                    st.download_button(
                        label="Download CSV",
                        data=f,
                        file_name=csv_file,
                        mime='text/csv'
                    )
        else:
            st.write("No failures found in the provided log files.")
    else:
        st.error("The provided folder path does not exist.")
else:
    st.write("Please enter a folder path to start parsing.")

