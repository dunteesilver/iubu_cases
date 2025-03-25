import pandas as pd
import datetime
import os
import pysftp
import paramiko

def create_filename():
    """
    Creates a filename with the format SF_HCP_Cases_Details_YYYYMMDD.csv.

    Returns:
        str: The generated filename.
    """
    today = datetime.date.today()
    formatted_date = today.strftime("%Y%m%d")
    filename = f"SF_HCP_Cases_Details_{formatted_date}.csv"
    return filename

def get_host_key(host, port, username, password):
    """Retrieves the host key from an SSH/SFTP server."""
    try:
        transport = paramiko.Transport((host, port))
        transport.connect(username=username, password=password)
        hostkey = transport.get_remote_server_key()
        transport.close()
        return hostkey
    except Exception as e:
        print(f"Error retrieving host key: {e}")
        return None

def add_host_key_to_known_hosts(host_key, host, known_hosts_path="~/.ssh/known_hosts"):
    """Adds the host key to the known_hosts file."""
    try:
        known_hosts = paramiko.HostKeys()
        known_hosts_path = os.path.expanduser(known_hosts_path)
        if os.path.exists(known_hosts_path):
            known_hosts.load(known_hosts_path)

        known_hosts.add(host, "ssh-rsa", host_key) #or "ssh-ed25519" depending on the server key.

        known_hosts.save(known_hosts_path)
        print(f"Host key added to {known_hosts_path}")

    except Exception as e:
        print(f"Error adding host key: {e}")
        
# function to connect to ftp
def sftp_upload(host, username, password, port, local_filepath, remote_file_name, host_key):
    """
    Connects to an SFTP server and uploads a file.

    Args:
        host (str): SFTP server hostname or IP address.
        username (str): SFTP username.
        password (str): SFTP password.
        port (int): SFTP port number.
        local_filepath (str): Path to the local file to upload.
        remote_file_name (str): name of the remote file (placed at root dir).
        known_hosts_path (str, optional): path to the known_hosts file.
    """
    if host_key:
        add_host_key_to_known_hosts(host_key, host)
    try:
        cnopts = pysftp.CnOpts()
 
        with pysftp.Connection(host=host, username=username, password=password, port=port, cnopts=cnopts) as sftp:
            print(f"Connected to SFTP server: {host}:{port}")

            # Check if the local file exists
            if not os.path.exists(local_filepath):
                print(f"Error: Local file not found: {local_filepath}")
                return

            sftp.put(local_filepath, remote_file_name)
            print(f"File uploaded successfully: {local_filepath} -> {remote_file_name}")

    except pysftp.ConnectionException as e:
        print(f"SFTP connection error: {e}")
    except pysftp.CredentialException as e:
        print(f"SFTP credential error: {e}")
    except pysftp.SSHException as e:
        print(f"SSH error: {e}")
    except FileNotFoundError as e:
        print(f"File not found error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        
def process_npi_data():
    try:
        cwd = os.getcwd()
        csv_file_path = os.path.join(cwd, "iubu_hcp_cases.csv")
        df = pd.read_csv(csv_file_path, dtype=str)

        # Keep current_month as a string (e.g., "March")
        current_month = datetime.datetime.now().strftime("%B")  

        # Convert current_month to its numeric value (e.g., "March" → "03")
        numeric_month = datetime.datetime.strptime(current_month, "%B").strftime("%m")

        try:
            df[f"Actuals_{current_month}__c"] = pd.to_numeric(df[f"Actuals_{current_month}__c"], errors='coerce').fillna(0).astype(int)
        except ValueError:
            print(f"Warning: Could not convert Actuals_{current_month}__c to numeric.")

        columns_to_keep = [
            "Contact_Profile_NPI__c",
            "IUBU_Contact_Profile__r.Contact__r.FirstName",
            "IUBU_Contact_Profile__r.Contact__r.LastName",
            "Date__c",
            f"Actuals_{current_month}__c",
            "Owner.Name",
            "Owner.Email",
            "Territory__c",
            "Territory_Name__c",
            "Manager_First_Name__c",
            "Manager_Last_Name__c",
            "Manager_Email__c",
            "Fiscal_Year__c",
        ]

        df_filtered = df[columns_to_keep]
        df_filtered = df_filtered.rename(columns={f"Actuals_{current_month}__c": "Actuals"})
        print(df_filtered)
        df_filtered = df_filtered[df_filtered["Actuals"].notna() & (df_filtered["Actuals"] != 0)]
        try:
            df_filtered["Fiscal_Year__c"] = pd.to_numeric(df_filtered["Fiscal_Year__c"], errors='coerce').fillna(0).astype(int)
        except ValueError:
            print(f"Warning: Could not convert Fiscal_Year__c to numeric.")

        # Convert Fiscal_Year__c to string for concatenation
        df_filtered["Fiscal_Year__c"] = df_filtered["Fiscal_Year__c"].astype(str)

        # Update Date__c to be "YYYY-MM-01" where YYYY is Fiscal_Year__c and MM is numeric_month
        df_filtered["Date__c"] = df_filtered["Fiscal_Year__c"] + "-" + numeric_month

        # Convert all columns to strings before fillna
        for col in df_filtered.columns:
            if col not in ["Actuals", "Fiscal_Year__c"]:
                df_filtered[col] = df_filtered[col].astype(str)

        df_grouped = df_filtered.groupby("Contact_Profile_NPI__c").agg({
            "IUBU_Contact_Profile__r.Contact__r.FirstName": 'first',
            "IUBU_Contact_Profile__r.Contact__r.LastName": 'first',
            "Date__c": 'first',  # Updated Date__c is included here
            "Actuals": 'sum',
            "Owner.Name": 'first',
            "Owner.Email": 'first',
            "Territory__c": 'first',
            "Territory_Name__c": 'first',
            "Manager_First_Name__c": 'first',
            "Manager_Last_Name__c": 'first',
            "Manager_Email__c": 'first',
            "Fiscal_Year__c": 'first'
        }).reset_index()

        cols = df_grouped.columns.tolist()
        cols.remove("Actuals")
        cols.remove("Fiscal_Year__c")
        cols.insert(cols.index("Date__c") + 1, "Actuals")
        df_grouped = df_grouped[cols]

        # Replace NaN values with empty strings
        df_grouped = df_grouped.astype(str).fillna('')
        df_grouped = df_grouped.replace("nan", "").replace("NaN", "").replace("None", "")

        df_grouped.rename(columns={"Contact_Profile_NPI__c": "NPI", "IUBU_Contact_Profile__r.Contact__r.FirstName": "HCPFirstName",
            "IUBU_Contact_Profile__r.Contact__r.LastName": "HCPLastName", "Date__c": "Month", "Actuals": "CasesCompleted",
            "Owner.Name": "HCP", "Owner.Email": "UCEmail", "Territory__c": "TerritoryCode", "Territory_Name__c": "TerritoryName",
            "Manager_First_Name__c": "RBMFirstName", "Manager_Last_Name__c": "RBMLastName", "Manager_Email__c": "RBMEmail"}, inplace=True)

        output_filename = create_filename()
        
        df_grouped.to_csv(output_filename, index=False, na_rep='')
        print("NPI data processed and saved to processed_npi_data.csv")

    except FileNotFoundError:
        print("Error: iubu_hcp_cases.csv not found in the current directory.")
    except Exception as e:
        print(f"An error occurred: {e}")

process_npi_data()

# new file path
output_filename = create_filename()
cwd = os.getcwd()
new_file_path = os.path.join(cwd, output_filename)

# send file to ftp site
host = "uroliftgateway.sftp.wpengine.com"
username = "uroliftgateway-vendordata"
password = "UL_data_$2022"
port = 2222  # Default SFTP port is 22
local_file = new_file_path
remote_file_name = output_filename

host_key = get_host_key(host, port, username, password)

sftp_upload(host, username, password, port, local_file, remote_file_name, host_key)
