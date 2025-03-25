import pandas as pd
import datetime
import os

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
        df_filtered = df_filtered[df_filtered["Actuals"].notna() & (df_filtered["Actuals"] != 0)]

        try:
            df_filtered["Fiscal_Year__c"] = pd.to_numeric(df_filtered["Fiscal_Year__c"], errors='coerce').fillna(0).astype(int)
        except ValueError:
            print(f"Warning: Could not convert Fiscal_Year__c to numeric.")

        # Convert Fiscal_Year__c to string for concatenation
        df_filtered["Fiscal_Year__c"] = df_filtered["Fiscal_Year__c"].astype(str)

        # Update Date__c to be "YYYY-MM-01" where YYYY is Fiscal_Year__c and MM is numeric_month
        df_filtered["Date__c"] = df_filtered["Fiscal_Year__c"] + "-" + numeric_month + "-01"

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
        
        df_grouped.to_csv("processed_npi_data.csv", index=False, na_rep='')
        print("NPI data processed and saved to processed_npi_data.csv")

    except FileNotFoundError:
        print("Error: iubu_hcp_cases.csv not found in the current directory.")
    except Exception as e:
        print(f"An error occurred: {e}")

process_npi_data()