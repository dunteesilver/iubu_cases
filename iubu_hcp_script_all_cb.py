import pandas as pd
import datetime
import os

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

def process_monthly_data(df, month_name):
    """Processes data for a single month and returns a DataFrame."""

    numeric_month = datetime.datetime.strptime(month_name, "%B").strftime("%m")

    try:
        df[f"Actuals_{month_name}__c"] = pd.to_numeric(df[f"Actuals_{month_name}__c"], errors='coerce').fillna(0).astype(int)
    except ValueError:
        print(f"Warning: Could not convert Actuals_{month_name}__c to numeric.")

    columns_to_keep = [
        "Contact_Profile_NPI__c",
        "IUBU_Contact_Profile__r.Contact__r.FirstName",
        "IUBU_Contact_Profile__r.Contact__r.LastName",
        "Date__c",
        f"Actuals_{month_name}__c",
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
    df_filtered = df_filtered.rename(columns={f"Actuals_{month_name}__c": "Actuals"})
    df_filtered = df_filtered[df_filtered["Actuals"].notna() & (df_filtered["Actuals"] != 0)]

    try:
        df_filtered["Fiscal_Year__c"] = pd.to_numeric(df_filtered["Fiscal_Year__c"], errors='coerce').fillna(0).astype(int)
    except ValueError:
        print(f"Warning: Could not convert Fiscal_Year__c to numeric.")

    df_filtered["Fiscal_Year__c"] = df_filtered["Fiscal_Year__c"].astype(str)
    df_filtered["Date__c"] = df_filtered["Fiscal_Year__c"] + "-" + numeric_month + "-01"

    for col in df_filtered.columns:
        if col not in ["Actuals", "Fiscal_Year__c"]:
            df_filtered[col] = df_filtered[col].astype(str)

    df_grouped = df_filtered.groupby(["Contact_Profile_NPI__c", "Date__c"]).agg({
        "IUBU_Contact_Profile__r.Contact__r.FirstName": 'first',
        "IUBU_Contact_Profile__r.Contact__r.LastName": 'first',
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

    df_grouped = df_grouped.astype(str).fillna('')
    df_grouped = df_grouped.replace("nan", "").replace("NaN", "").replace("None", "")

    return df_grouped

def process_all_months():
    """Processes data for all 12 months and returns a concatenated DataFrame."""

    cwd = os.getcwd()
    csv_file_path = os.path.join(cwd, "iubu_hcp_cases.csv")
    df = pd.read_csv(csv_file_path, dtype=str)

    all_months_data = []  # List to store DataFrames for each month

    months = [
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December"
    ]

    for month in months:
        monthly_df = process_monthly_data(df.copy(), month) #copy to avoid modifying original dataframe
        all_months_data.append(monthly_df)

    final_df = pd.concat(all_months_data, ignore_index=True)
    return final_df

# Example usage:
final_data = process_all_months()

output_filename = create_filename()
final_data.to_csv(output_filename, index=False, na_rep='')
print(f"Data for all months saved to {output_filename}")
cwd = os.getcwd()
new_file_path = os.path.join(cwd, output_filename)