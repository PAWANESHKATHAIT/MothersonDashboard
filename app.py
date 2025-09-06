import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import numpy as np
import os
import json # You need to import the json library

# Set up Google Sheets API
try:
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    
    # Access the credentials from Streamlit secrets instead of a local file
    creds_dict = st.secrets["gcp_service_account"]
    
    # Use from_json_keyfile_dict to load from a dictionary, not a file
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    
    client = gspread.authorize(creds)
    spreadsheet = client.open("Swipe Dashboard Data")
    st.sidebar.success("Successfully connected to Google Sheets! ✅")
except Exception as e:
    st.sidebar.error(f"Failed to connect to Google Sheets. Please ensure your credentials are set correctly in Streamlit's secrets.")
    st.sidebar.info("Error details: " + str(e))
    st.stop()

# All your other functions and the main application logic remain the same
st.title("Motherson Enterprises Data Database Builder 📊")
st.write("Upload multiple Excel files to build a comprehensive database.")

def clean_dataframe_for_json(df, sheet_type="Sales"):
    # ... (Your existing code)
    df = df.copy()
    
    standard_headers = ["S.no.", "Date", "Invoice No.", "GSTIN", "Particulars", 
                        "Quantity", "Rate", "Quantity *Rate", "% of GST", 
                        "CGST", "SGST", "IGST", "Total"]
    
    clean_df = pd.DataFrame(columns=standard_headers)
    
    column_mapping = {}
    df_columns_lower = [col.lower().strip() for col in df.columns]
    
    for std_header in standard_headers:
        std_header_lower = std_header.lower().strip()
        
        for i, col_lower in enumerate(df_columns_lower):
            original_col = df.columns[i]
            
            if (std_header_lower == col_lower or 
                std_header_lower.replace('.', '').replace(' ', '') == col_lower.replace('.', '').replace(' ', '') or
                (std_header_lower == 'quantity *rate' and ('total' in col_lower and 'quantity' in col_lower)) or
                (std_header_lower == '% of gst' and ('gst' in col_lower and '%' in col_lower)) or
                (std_header_lower == 's.no.' and ('s.no' in col_lower or 'sno' in col_lower or 'serial' in col_lower))):
                column_mapping[std_header] = original_col
                break

    for std_header in standard_headers: 
        if std_header in column_mapping:
            clean_df[std_header] = df[column_mapping[std_header]]
        else:
            clean_df[std_header] = ""
    
    if clean_df["S.no."].isnull().all() or (clean_df["S.no."] == "").all():
        clean_df["S.no."] = range(1, len(clean_df) + 1)
    
    clean_df = clean_df.astype(str)
    
    numerical_cols = ["S.no.", "Quantity", "Rate", "Quantity *Rate", "% of GST", "CGST", "SGST", "IGST", "Total"]
    
    for col in numerical_cols:
        if col in clean_df.columns:
            clean_df[col] = pd.to_numeric(clean_df[col], errors='coerce')
            clean_df[col] = clean_df[col].replace([np.inf, -np.inf], np.nan)
            clean_df[col] = clean_df[col].fillna(0)
            max_value = 1e15
            clean_df[col] = clean_df[col].clip(lower=-max_value, upper=max_value)
            if col == "S.no.":
                clean_df[col] = clean_df[col].round(0).astype(int)
            else:
                clean_df[col] = clean_df[col].round(2)
    
    if "Date" in clean_df.columns:
        clean_df["Date"] = pd.to_datetime(clean_df["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
        clean_df["Date"] = clean_df["Date"].fillna("")
    
    for col in clean_df.columns:
        if col not in numerical_cols and col != "Date":
            clean_df[col] = clean_df[col].astype(str)
            clean_df[col] = clean_df[col].replace(['nan', 'NaN', 'None'], '')
    
    clean_df = clean_df.fillna("")
    
    mask = pd.Series([True] * len(clean_df), index=clean_df.index)
    
    if "Particulars" in clean_df.columns:
        particulars_lower = clean_df["Particulars"].astype(str).str.lower().str.strip()
        mask = mask & ~particulars_lower.str.contains('total|grand|sum', na=False, regex=True)
    
    important_cols = ["Invoice No.", "Particulars", "Quantity", "Rate", "Total"]
    available_important_cols = [col for col in important_cols if col in clean_df.columns]
    
    if available_important_cols:
        for idx in clean_df.index:
            all_empty = True
            for col in available_important_cols:
                value = clean_df.loc[idx, col]
                if col in ["Quantity", "Rate", "Total"]:
                    if pd.notna(value) and value != 0:
                        all_empty = False
                        break
                else:
                    if pd.notna(value) and str(value).strip() != "" and str(value).strip().lower() != "nan":
                        all_empty = False
                        break
            
            if all_empty:
                mask.loc[idx] = False
    
    clean_df = clean_df[mask].copy()
    
    if not clean_df.empty:
        clean_df = clean_df.reset_index(drop=True)
        clean_df["S.no."] = range(1, len(clean_df) + 1)
    
    for col in numerical_cols:
        if col in clean_df.columns:
            if col == "S.no.":
                clean_df[col] = clean_df[col].astype(int)
            else:
                clean_df[col] = clean_df[col].astype(float)
    
    return clean_df

def safe_upload_to_sheets(sheet, df, sheet_name):
    # ... (Your existing code)
    try:
        existing_data = sheet.get_all_values()
        
        last_row_with_data = 1
        for i, row in enumerate(existing_data):
            if i == 0:
                continue
            if any(cell.strip() for cell in row if cell.strip()):
                last_row_with_data = i + 1
        
        next_row = last_row_with_data + 1
        
        df = df.replace([np.nan, np.inf, -np.inf], 0)
        
        values = df.values.tolist()
        
        if 'S.no.' in df.columns:
            start_serial = last_row_with_data
            for i in range(len(values)):
                values[i][df.columns.get_loc('S.no.')] = start_serial + i + 1

        if values:
            end_col = chr(65 + len(df.columns) - 1)
            end_row = next_row + len(values) - 1
            range_name = f"A{next_row}:{end_col}{end_row}"
            sheet.update(range_name, values)
        
        return True, None, len(values)
        
    except Exception as e:
        return False, str(e), 0

uploaded_file = st.file_uploader("Upload Swipe Excel File", type=["xlsx"])

if uploaded_file:
    try:
        xls = pd.ExcelFile(uploaded_file)
        
        if "Sales" not in xls.sheet_names or "Purchases" not in xls.sheet_names:
            st.error("Error: The Excel file is missing the 'Sales' or 'Purchases' sheets. Please ensure both sheets are present.")
        else:
            sales_df = pd.read_excel(xls, "Sales")
            purchases_df = pd.read_excel(xls, "Purchases")

            sales_df = clean_dataframe_for_json(sales_df, "Sales")
            purchases_df = clean_dataframe_for_json(purchases_df, "Purchases")

            st.subheader("Sales Data Preview (Standardized)")
            st.dataframe(sales_df.head())
            
            st.subheader("Purchases Data Preview (Standardized)")
            st.dataframe(purchases_df.head())
            
            if len(sales_df) == 0 and len(purchases_df) == 0:
                st.warning("⚠️ No valid data found after filtering. Please check your Excel file format.")
                st.stop()

            if st.button("Upload to Google Sheets 🚀"):
                with st.spinner('Uploading data...'):
                    try:
                        sales_sheet = spreadsheet.worksheet("Sales")
                        purchases_sheet = spreadsheet.worksheet("Purchases")
                        
                        headers = ["S.no.", "Date", "Invoice No.", "GSTIN", "Particulars", 
                                   "Quantity", "Rate", "Quantity *Rate", "% of GST", 
                                   "CGST", "SGST", "IGST", "Total"]
                        
                        try:
                            sales_first_row = sales_sheet.row_values(1)
                            if not sales_first_row or len(sales_first_row) < 13:
                                sales_sheet.update("A1:M1", [headers])
                        except:
                            sales_sheet.update("A1:M1", [headers])
                        
                        try:
                            purchases_first_row = purchases_sheet.row_values(1)
                            if not purchases_first_row or len(purchases_first_row) < 13:
                                purchases_sheet.update("A1:M1", [headers])
                        except:
                            purchases_sheet.update("A1:M1", [headers])
                        
                        success_sales, error_sales, rows_added_sales = safe_upload_to_sheets(sales_sheet, sales_df, "Sales")
                        
                        success_purchases, error_purchases, rows_added_purchases = safe_upload_to_sheets(purchases_sheet, purchases_df, "Purchases")
                        
                        if success_sales and success_purchases:
                            total_sales_rows = len(sales_sheet.get_all_values()) - 1
                            total_purchases_rows = len(purchases_sheet.get_all_values()) - 1
                            
                            st.success("🎉 Data successfully added to database!")
                            st.info("📊 **Database Summary:**")
                            st.write(f"• **Sales Database**: {rows_added_sales} new rows added | **Total rows now**: {total_sales_rows}")
                            st.write(f"• **Purchases Database**: {rows_added_purchases} new rows added | **Total rows now**: {total_purchases_rows}")
                            st.write("• **Serial Numbers**: Automatically continued from existing data")
                            
                    except Exception as e:
                        st.error(f"An error occurred during upload: {str(e)}")
                        st.info("This might be due to permission issues or connection problems. Please check your Google Sheets access.")
                
    except Exception as e:
        st.error(f"An error occurred while processing the Excel file. Please check the file's format and try again.")
        st.info(f"Error details: {e}")