import streamlit as st
import pandas as pd
import gspread
import numpy as np
from oauth2client.service_account import ServiceAccountCredentials

# --- Function to clean and upload data ---
def safe_upload_to_sheets(sheet, df, sheet_name):
    """Safely append dataframe to Google Sheets (building a database)"""
    try:
        # Get existing data to find the next available row
        existing_data = sheet.get_all_values()
        last_row_with_data = 1
        for i, row in enumerate(existing_data):
            if i == 0:
                continue
            if any(cell.strip() for cell in row if cell.strip()):
                last_row_with_data = i + 1
        
        next_row = last_row_with_data + 1

        # Replace non-compliant JSON values like inf, -inf, and NaN with 0
        df = df.replace([np.nan, np.inf, -np.inf], 0)
        
        # Ensure that all numerical columns are valid numbers before conversion
        numerical_cols = ["S.no.", "Quantity", "Rate", "Quantity *Rate", "% of GST", "CGST", "SGST", "IGST", "Total"]
        for col in numerical_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        values = df.values.tolist()
        
        # Update S.no. column to be sequential
        if 'S.no.' in df.columns:
            start_serial = last_row_with_data
            for i in range(len(values)):
                values[i][df.columns.get_loc('S.no.')] = int(start_serial + i + 1)

        if values:
            end_col = chr(65 + len(df.columns) - 1)
            end_row = next_row + len(values) - 1
            range_name = f"A{next_row}:{end_col}{end_row}"
            sheet.update(range_name, values)
        
        return True, None, len(values)
        
    except Exception as e:
        return False, str(e), 0

# --- Streamlit App UI ---
st.title("Upload your Swipe Excel file to extract, clean, and upload Sales and Purchases data to Google Sheets.")

# --- Google Sheets Connection (Corrected) ---
try:
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = st.secrets["gcp_service_account"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    spreadsheet = client.open("Swipe Dashboard Data")
    st.sidebar.success("Successfully connected to Google Sheets! ✅")

except Exception as e:
    st.sidebar.error(f"Failed to connect to Google Sheets. Please ensure your credentials are set correctly in Streamlit's secrets.\n\nError details: {e}")
    st.stop()


# --- File Upload and Processing ---
uploaded_file = st.file_uploader("Drag and drop file here", type=['xlsx'], help="Limit 200MB per file")

if uploaded_file:
    # --- Data Cleaning and Processing (Sales) ---
    try:
        sales_df = pd.read_excel(uploaded_file, sheet_name="Sales")
        
        # Rename columns to match Google Sheet headers
        sales_df = sales_df.rename(columns={
            "S. No.": "S.no.",
            "Invoice No.": "Invoice No.",
            "Date": "Date",
            "GSTIN": "GSTIN",
            "Particulars": "Particulars",
            "Quantity": "Quantity",
            "Rate": "Rate",
            "Quantity *Rate": "Quantity *Rate",
            "% of GST": "% of GST",
            "CGST": "CGST",
            "SGST": "SGST",
            "IGST": "IGST",
            "Total": "Total"
        })
        
        # Filter out rows with "Total" in the 'Particulars' column
        sales_df = sales_df[sales_df['Particulars'].str.lower() != 'total'].copy()

        # Upload sales data
        sales_sheet = spreadsheet.worksheet("Sales")
        sales_success, sales_error, sales_rows_uploaded = safe_upload_to_sheets(sales_sheet, sales_df, "Sales")
        
        if sales_success:
            st.success(f"Successfully uploaded {sales_rows_uploaded} rows to the 'Sales' sheet! 🎉")
        else:
            st.error("An error occurred while processing the Excel file. Please check the file's format and try again.")
            st.error(f"Error details: {sales_error}")

    except Exception as e:
        st.error("An error occurred while processing the Excel file. Please check the file's format and try again.")
        st.error(f"Error details: {e}")

    # --- Data Cleaning and Processing (Purchases) ---
    try:
        purchases_df = pd.read_excel(uploaded_file, sheet_name="Purchases")

        # Rename columns to match Google Sheet headers
        purchases_df = purchases_df.rename(columns={
            "S. No.": "S.no.",
            "Invoice No.": "Invoice No.",
            "Date": "Date",
            "GSTIN": "GSTIN",
            "Particulars": "Particulars",
            "Quantity": "Quantity",
            "Rate": "Rate",
            "Quantity *Rate": "Quantity *Rate",
            "% of GST": "% of GST",
            "CGST": "CGST",
            "SGST": "SGST",
            "IGST": "IGST",
            "Total": "Total"
        })

        # Filter out rows with "Total" in the 'Particulars' column
        purchases_df = purchases_df[purchases_df['Particulars'].str.lower() != 'total'].copy()

        # Upload purchases data
        purchases_sheet = spreadsheet.worksheet("Purchases")
        purchases_success, purchases_error, purchases_rows_uploaded = safe_upload_to_sheets(purchases_sheet, purchases_df, "Purchases")

        if purchases_success:
            st.success(f"Successfully uploaded {purchases_rows_uploaded} rows to the 'Purchases' sheet! 🎉")
        else:
            st.error("An error occurred while processing the Excel file. Please check the file's format and try again.")
            st.error(f"Error details: {purchases_error}")

    except Exception as e:
        st.error("An error occurred while processing the Excel file. Please check the file's format and try again.")
        st.error(f"Error details: {e}")