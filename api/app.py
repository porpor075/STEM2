from flask import Flask, render_template, jsonify, url_for, request, redirect, session, make_response
from flask_compress import Compress
import pandas as pd
import numpy as np
import os
import requests
from functools import wraps
import sys
import traceback
import math
from datetime import datetime

# Get the directory where app.py is located
basedir = os.path.abspath(os.path.dirname(__file__))

app = Flask(__name__, 
            static_folder=os.path.join(basedir, "../static"), 
            template_folder=os.path.join(basedir, "../templates"))
Compress(app) # Enable Gzip compression
application = app # Add this for Vercel WSGI entry point
app.secret_key = "conicle_secret_key_for_session"

# Global variables to act as a simple in-memory cache
_cached_df = None
_cached_time = None
_cache_expiry = 300 # 5 minutes

# Cache สำหรับ Calling List จาก GAS
_cached_gas_df = None
_cached_gas_time = None

# URLs
DATA_SHEET_URL = "https://docs.google.com/spreadsheets/d/18YTiZ67OCVvTXltashgTPIvpH6u8e6GnAb986PctVOI/export?format=csv&gid=0"
AUTH_SHEET_URL = "https://docs.google.com/spreadsheets/d/18YTiZ67OCVvTXltashgTPIvpH6u8e6GnAb986PctVOI/export?format=csv&gid=56353457"
USER_ACCESS_URL = "https://docs.google.com/spreadsheets/d/18YTiZ67OCVvTXltashgTPIvpH6u8e6GnAb986PctVOI/export?format=csv&gid=94070811"
CALLING_LIST_URL = "https://docs.google.com/spreadsheets/d/18YTiZ67OCVvTXltashgTPIvpH6u8e6GnAb986PctVOI/export?format=csv&gid=2097893051"
ISSUE_SHEET_URL = "https://docs.google.com/spreadsheets/d/18YTiZ67OCVvTXltashgTPIvpH6u8e6GnAb986PctVOI/export?format=csv&gid=1570354642"
FB_ADS_URL = "https://docs.google.com/spreadsheets/d/11aluJj_MKnEuTSuvkcY-LtWjMksNexKZU0lcM70A8u4/export?format=csv&gid=0"
CAMPAIGN_REG_URL = "https://docs.google.com/spreadsheets/d/11aluJj_MKnEuTSuvkcY-LtWjMksNexKZU0lcM70A8u4/export?format=csv&gid=492892797"
LEARNDI_SHEET_URL = "https://docs.google.com/spreadsheets/d/1UOPv0_BZ-ueqfK2Jjhs6AdvqFNiE1tiKcRVvTO-KMHE/export?format=csv&gid=459253712"

# Paths
REPORT_CSV = "/tmp/STEM_Learning_Report.csv"
REMOTE_CSV = "/tmp/googlesheet_report.csv"
AUTH_CSV = "/tmp/auth_users.csv"
USER_ACCESS_CSV = "/tmp/user_access.csv"
CALLING_LIST_CSV = "/tmp/calling_list.csv"
ISSUE_CSV = "/tmp/issue_list.csv"
FB_ADS_CSV = "/tmp/fb_ads.csv"
CAMPAIGN_REG_CSV = "/tmp/campaign_reg.csv"
LEARNDI_CSV = "/tmp/learndi_report.csv"

# Google Apps Script Web App URL
GAS_URL = os.environ.get("GAS_URL", "https://script.google.com/macros/s/AKfycby0RE_RSrIeoyQxGDEMJKA1af-481o6xdJEOULTkmr9Pgxp40STd_3h8Q30BTfvjhwWrw/exec")

USER_AUTH = {"Conicle": "Conicle@33"}

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'logged_in' not in session: return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

def load_auth_credentials():
    global USER_AUTH
    try:
        # Add cache buster
        url = f"{AUTH_SHEET_URL}&t={int(datetime.now().timestamp())}"
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            with open(AUTH_CSV, 'wb') as f: f.write(resp.content)
            df_auth = pd.read_csv(AUTH_CSV)
            df_auth.columns = [c.lower().strip() for c in df_auth.columns]
            if 'username' in df_auth.columns and 'password' in df_auth.columns:
                df_auth['username'] = df_auth['username'].astype(str).str.strip()
                df_auth['password'] = df_auth['password'].astype(str).str.strip()
                new_auth = dict(zip(df_auth['username'], df_auth['password']))
                new_auth["Conicle"] = "Conicle@33"
                USER_AUTH = new_auth
    except Exception as e: print(f"Auth error: {e}")

def load_user_access_data():
    try:
        resp = requests.get(USER_ACCESS_URL, timeout=10)
        if resp.status_code == 200:
            with open(USER_ACCESS_CSV, 'wb') as f: f.write(resp.content)
            df = pd.read_csv(USER_ACCESS_CSV)
            df.columns = [c.strip() for c in df.columns]
            col_map = {c.lower(): c for c in df.columns}
            if 'email' in col_map and 'date_joined' in col_map:
                email_col = col_map['email']
                dj_col = col_map['date_joined']
                df['email'] = df[email_col].astype(str).str.strip().str.lower()
                df['date_joined'] = df[dj_col]
                return df[['email', 'date_joined']].drop_duplicates('email')
    except Exception as e: print(f"User access load error: {e}")
    return pd.DataFrame(columns=['email', 'date_joined'])

_cached_learndi_df = None
_cached_learndi_time = None

def get_learndi_data():
    global _cached_learndi_df, _cached_learndi_time
    if _cached_learndi_df is not None and _cached_learndi_time is not None:
        if (datetime.now() - _cached_learndi_time).total_seconds() < _cache_expiry:
            return _cached_learndi_df
    try:
        resp = requests.get(LEARNDI_SHEET_URL, timeout=15)
        if resp.status_code == 200:
            with open(LEARNDI_CSV, 'wb') as f: f.write(resp.content)
            df = pd.read_csv(LEARNDI_CSV)
            _cached_learndi_df = df
            _cached_learndi_time = datetime.now()
            return df
    except Exception as e:
        print(f"Error fetching Learndi data: {e}")
    return pd.DataFrame()

def process_data():
    if not os.path.exists(REMOTE_CSV): return False
    try:
        df_raw = pd.read_csv(REMOTE_CSV, low_memory=False)
        df_raw.columns = [c.strip() for c in df_raw.columns]
        
        # Mapping alternative names if needed
        col_map = {c.lower(): c for c in df_raw.columns}
        if 'email' in col_map and 'Email' not in df_raw.columns:
            df_raw.rename(columns={col_map['email']: 'Email'}, inplace=True)
            
        if 'Email' not in df_raw.columns:
            print("Error: 'Email' column missing in raw data")
            return False

        df_access = load_user_access_data()
        df_raw['Email'] = df_raw['Email'].astype(str).str.strip().str.lower()
        df_raw['Progress Percentage'] = pd.to_numeric(df_raw['Progress Percentage'], errors='coerce').fillna(0)
        
        if not df_access.empty:
            df_raw = df_raw.merge(df_access, left_on='Email', right_on='email', how='left').drop(columns=['email'])
        else:
            df_raw['date_joined'] = np.nan

        df_raw['Content Provider'] = df_raw['Content Provider'].fillna('Unknown')
        
        # เตรียมคอลัมน์วันที่สำคัญ
        date_fields = ['Added Date', 'Start Date', 'Completed Date']
        for field in date_fields:
            if field in df_raw.columns:
                df_raw[field] = pd.to_datetime(df_raw[field], errors='coerce').dt.strftime('%Y-%m-%d').fillna('-')
            else:
                df_raw[field] = '-'

        df_raw['Date_Parsed'] = pd.to_datetime(df_raw['Date'], errors='coerce')
        df_raw['Date_Str'] = df_raw['Date_Parsed'].dt.strftime('%Y-%m-%d')
        
        df_sorted = df_raw.sort_values(['Email', 'Content Name', 'Date_Parsed'], ascending=[True, True, False])
        latest_status = df_sorted.drop_duplicates(['Email', 'Content Name'])[['Email', 'Content Name', 'Transaction Status']]
        valid_enrollments = latest_status[latest_status['Transaction Status'] == 'Learner']
        df = df_raw.merge(valid_enrollments[['Email', 'Content Name']], on=['Email', 'Content Name'], how='inner').copy()
        
        index_cols = ['Email', 'First Name', 'Last Name', 'Content Name', 'Content Provider', 'date_joined', 'Completed Date', 'Added Date', 'Start Date']
        for col in index_cols: df[col] = df[col].fillna('-')

        pivot_df = df.pivot_table(index=index_cols, columns='Date_Str', values='Progress Percentage', aggfunc='max').reset_index()
        pivot_df.columns.name = None
        
        date_cols = sorted([col for col in pivot_df.columns if col not in index_cols])
        
        # Vectorized Status Determination
        pivot_numeric = pivot_df[date_cols].apply(pd.to_numeric, errors='coerce').fillna(0)
        max_progress = pivot_numeric.max(axis=1)

        pivot_df['Learning Status'] = 'Not Start'
        pivot_df.loc[max_progress > 0, 'Learning Status'] = 'In Progress'
        pivot_df.loc[max_progress >= 100, 'Learning Status'] = 'Completed'

        # Vectorized User Category
        pivot_df['User_Status_Category'] = '0'
        pivot_df.loc[max_progress >= 1, 'User_Status_Category'] = 'in-progress-early'
        pivot_df.loc[max_progress > 50, 'User_Status_Category'] = 'in-progress-high'
        pivot_df.loc[max_progress >= 100, 'User_Status_Category'] = '100'

        # Reorder columns
        metadata_cols = ['Email', 'First Name', 'Last Name', 'Content Name', 'Content Provider', 'date_joined', 'Learning Status', 'Completed Date', 'Added Date', 'Start Date']
        cols_order = metadata_cols + date_cols + ['User_Status_Category']
        pivot_df = pivot_df[cols_order]

        pivot_df.to_csv(REPORT_CSV, index=False, encoding='utf-8-sig')
        return True
    except Exception as e: 
        print(f"Process error: {e}")
        return False

def get_report_data():
    global _cached_df, _cached_time
    if _cached_df is not None and _cached_time is not None:
        if (datetime.now() - _cached_time).total_seconds() < _cache_expiry:
            return _cached_df
    if not os.path.exists(REPORT_CSV): refresh_logic()
    if os.path.exists(REPORT_CSV):
        try:
            _cached_df = pd.read_csv(REPORT_CSV, low_memory=False)
            _cached_df.columns = [c.strip() for c in _cached_df.columns]
            _cached_time = datetime.now()
            return _cached_df
        except: pass
    return pd.DataFrame()

def get_gas_calling_list():
    global _cached_gas_df, _cached_gas_time
    if _cached_gas_df is not None and _cached_gas_time is not None:
        if (datetime.now() - _cached_gas_time).total_seconds() < 10: # ลดเหลือ 10 วินาทีเพื่อให้ดู sync ไวขึ้น
            return _cached_gas_df
    df_calling = pd.DataFrame()
    try:
        if GAS_URL and "placeholder" not in GAS_URL.lower():
            resp = requests.get(f"{GAS_URL}?action=getCallingList", timeout=10)
            if resp.status_code == 200:
                gas_data = resp.json()
                if gas_data and isinstance(gas_data, list):
                    df_calling = pd.DataFrame(gas_data)
                    df_calling.columns = [c.strip() for c in df_calling.columns]
                    if 'Email' in df_calling.columns:
                        df_calling['Email'] = df_calling['Email'].astype(str).str.strip().str.lower()
                        _cached_gas_df = df_calling
                        _cached_gas_time = datetime.now()
    except Exception as e: print(f"Error fetching GAS calling list: {e}")
    return df_calling

def refresh_logic():
    global _cached_df, _cached_time, _cached_gas_df, _cached_gas_time
    global _cached_fb_df, _cached_fb_time, _cached_camp_df, _cached_camp_time
    
    _cached_df = None
    _cached_time = None
    _cached_gas_df = None
    _cached_gas_time = None
    _cached_fb_df = None
    _cached_fb_time = None
    _cached_camp_df = None
    _cached_camp_time = None
    
    try:
        if os.path.exists(REPORT_CSV): os.remove(REPORT_CSV)
        # Add cache busters
        ts = int(datetime.now().timestamp())
        r1 = requests.get(f"{DATA_SHEET_URL}&t={ts}", timeout=15)
        if r1.status_code == 200:
            with open(REMOTE_CSV, 'wb') as f: f.write(r1.content)
        r2 = requests.get(f"{USER_ACCESS_URL}&t={ts}", timeout=15)
        if r2.status_code == 200:
            with open(USER_ACCESS_CSV, 'wb') as f: f.write(r2.content)
        load_auth_credentials()
        return process_data()
    except Exception as e: return False

# Cache สำหรับ Facebook Ads
_cached_fb_df = None
_cached_fb_time = None

# Cache สำหรับ Campaign Registration
_cached_camp_df = None
_cached_camp_time = None

def get_campaign_reg_data():
    global _cached_camp_df, _cached_camp_time
    if _cached_camp_df is not None and _cached_camp_time is not None:
        if (datetime.now() - _cached_camp_time).total_seconds() < _cache_expiry:
            return _cached_camp_df
    try:
        # Add cache buster
        url = f"{CAMPAIGN_REG_URL}&t={int(datetime.now().timestamp())}"
        resp = requests.get(url, timeout=15)
        if resp.status_code == 200:
            with open(CAMPAIGN_REG_CSV, 'wb') as f: f.write(resp.content)
            df = pd.read_csv(CAMPAIGN_REG_CSV)
            if 'Timestamp' in df.columns:
                # Timestamp format is usually MM/DD/YYYY HH:MM:SS or DD/MM/YYYY
                df['Date'] = pd.to_datetime(df['Timestamp'], errors='coerce').dt.strftime('%Y-%m-%d')
                df = df.dropna(subset=['Date'])
                daily_camp = df.groupby('Date').size().reset_index(name='Count')
                _cached_camp_df = daily_camp
                _cached_camp_time = datetime.now()
                return daily_camp
    except Exception as e:
        print(f"Error fetching Campaign Reg data: {e}")
    return pd.DataFrame(columns=['Date', 'Count'])

def get_fb_ads_data():
    global _cached_fb_df, _cached_fb_time
    if _cached_fb_df is not None and _cached_fb_time is not None:
        if (datetime.now() - _cached_fb_time).total_seconds() < _cache_expiry:
            return _cached_fb_df
    try:
        # Add cache buster
        url = f"{FB_ADS_URL}&t={int(datetime.now().timestamp())}"
        resp = requests.get(url, timeout=15)
        if resp.status_code == 200:
            with open(FB_ADS_CSV, 'wb') as f: f.write(resp.content)
            df = pd.read_csv(FB_ADS_CSV)
            # Normalize column names
            df.columns = [c.strip() for c in df.columns]
            
            # Ensure numeric columns are actually numeric
            numeric_cols = ['Amount spent (THB)', 'Reach', 'Impressions', 'Link clicks']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                else:
                    df[col] = 0

            # Convert date
            if 'Reporting starts' in df.columns:
                df['Date'] = pd.to_datetime(df['Reporting starts'], errors='coerce').dt.strftime('%Y-%m-%d')
                df = df.dropna(subset=['Date'])
                
                # Group by date to get daily spend and metrics
                daily_ads = df.groupby('Date').agg({
                    'Amount spent (THB)': 'sum',
                    'Reach': 'sum',
                    'Impressions': 'sum',
                    'Link clicks': 'sum'
                }).reset_index()
                
                _cached_fb_df = daily_ads
                _cached_fb_time = datetime.now()
                return daily_ads
    except Exception as e:
        print(f"Error fetching FB Ads data: {e}")
    return pd.DataFrame(columns=['Date', 'Amount spent (THB)', 'Reach', 'Impressions', 'Link clicks'])

@app.route('/')
@login_required
def summary():
    try:
        df = get_report_data()
        
        # Fetch FB Ads data
        fb_df = get_fb_ads_data()
        # Fetch Campaign Registration data
        camp_df = get_campaign_reg_data()
        # Fetch Learndi data
        learndi_df = get_learndi_data()
        learndi_stats = learndi_df.fillna('-').to_dict(orient='records') if not learndi_df.empty else []
        
        # --- REVENUE CALCULATION ---
        revenue_data = []
        grand_total_revenue = 0
        
        try:
            # 1. Fetch Price/Quota Data
            price_url = f"https://docs.google.com/spreadsheets/d/1UOPv0_BZ-ueqfK2Jjhs6AdvqFNiE1tiKcRVvTO-KMHE/export?format=csv&gid=0&t={int(datetime.now().timestamp())}"
            price_resp = requests.get(price_url, timeout=15)
            if price_resp.status_code == 200:
                price_df = pd.read_csv(io.StringIO(price_resp.text))
                
                # 2. Pre-calculate STEM Completion Data
                temp_c_stats = {}
                if not df.empty and 'Content Name' in df.columns:
                    # Filter for Completed and group by Content Name
                    stem_compl_counts = df[df['Learning Status'] == 'Completed']['Content Name'].value_counts().to_dict()
                    temp_c_stats = stem_compl_counts
                
                # Map Learndi Course Name -> Completed Count
                learndi_compl = {}
                if not learndi_df.empty:
                    # Try to find 'Course' and 'Done' columns
                    l_cols = {c.strip(): c for c in learndi_df.columns}
                    c_col = l_cols.get('Course')
                    d_col = l_cols.get('Done')
                    if c_col and d_col:
                        for _, row in learndi_df.iterrows():
                            cn = str(row[c_col]).strip()
                            done_raw = str(row[d_col]).replace(',', '').strip()
                            done = pd.to_numeric(done_raw, errors='coerce') or 0
                            learndi_compl[cn] = done

                # 3. Calculate Revenue per Course
                for _, p_row in price_df.iterrows():
                    course = str(p_row.get('Course', '')).strip()
                    if not course or course == 'nan': continue
                    
                    quota = str(p_row.get('Quota', '0')).replace(',', '').strip()
                    quota = float(pd.to_numeric(quota, errors='coerce') or 0)
                    
                    price_boi = str(p_row.get('Price BOI', '0')).replace(',', '').strip()
                    price_boi = float(pd.to_numeric(price_boi, errors='coerce') or 0)
                    
                    bonus_per_person = str(p_row.get('Price / Content / Complete', '0')).replace(',', '').strip()
                    bonus_per_person = float(pd.to_numeric(bonus_per_person, errors='coerce') or 0)
                    
                    stem_done = temp_c_stats.get(course, 0)
                    learndi_done = learndi_compl.get(course, 0)
                    
                    # Logic: STEM users priority for Quota
                    billable_stem = min(float(stem_done), quota)
                    remaining_quota = max(0.0, quota - billable_stem)
                    billable_learndi = min(float(learndi_done), remaining_quota)
                    
                    base_rev = billable_stem * (price_boi * 0.20)
                    bonus_rev = (billable_stem + billable_learndi) * bonus_per_person
                    
                    total_course_rev = base_rev + bonus_rev
                    grand_total_revenue += total_course_rev
                    
                    revenue_data.append({
                        "Course": course,
                        "STEM_Done": int(stem_done),
                        "Learndi_Done": int(learndi_done),
                        "Quota": int(quota),
                        "Billable_STEM": int(billable_stem),
                        "Billable_Learndi": int(billable_learndi),
                        "Base_Revenue": base_rev,
                        "Bonus_Revenue": bonus_rev,
                        "Total": total_course_rev
                    })
            else:
                print(f"Price sheet fetch failed: {price_resp.status_code}")
        except Exception as e:
            print(f"Revenue calc error: {e}")
            traceback.print_exc()

        if df.empty or 'Email' not in df.columns:
            # Provide empty defaults to prevent template errors
            return render_template('summary.html', total_users=0, joined_users_count=0,
                               users_100_count=0, users_51_99_count=0, 
                               users_1_50_count=0, users_0_count=0, 
                               all_users_list=[], content_stats=[],
                               latest_date=None,
                               daily_completion_data={"labels":[], "datasets":[]},
                               daily_start_data={"labels":[], "values":[]},
                               comparison_data={"labels":[], "added":[], "started":[], "completed":[]},
                               learning_comparison_data={"labels":[], "datasets":[]},
                               fb_grad_data={"labels":[], "spend":[], "completions":[]},
                               fb_start_data={"labels":[], "starts":[], "reach":[], "impressions":[], "cpr":[], "added":[], "completions":[], "campaign":[]},
                               avg_days=0, median_days=0, mode_days=0, min_days=0, max_days=0,
                               chart_labels=[], chart_values=[], daily_stats=[],
                               learndi_stats=learndi_stats,
                               revenue_data=revenue_data,
                               grand_total_revenue=grand_total_revenue)
        
        meta = ['Email', 'First Name', 'Last Name', 'Content Name', 'Content Provider', 'date_joined', 'Learning Status', 'User_Status_Category', 'Completed Date', 'Added Date', 'Start Date']
        date_cols = [c for c in df.columns if c not in meta]
        latest_date = date_cols[-1] if date_cols else None
        
        total_unique = df['Email'].nunique()
        joined_dates = pd.to_datetime(df.drop_duplicates('Email')['date_joined'], errors='coerce').dropna().sort_values().dt.strftime('%Y-%m-%d')
        joined_count = len(joined_dates)

        numeric_dates = df[date_cols].apply(pd.to_numeric, errors='coerce').fillna(0)
        user_max_per_date = pd.concat([df['Email'], numeric_dates], axis=1).groupby('Email').max()
        user_max = user_max_per_date.max(axis=1)
        
        compl_df = df[df['Completed Date'] != '-'].copy()
        all_comp_dates = sorted(compl_df['Completed Date'].unique().tolist())
        daily_data = {"labels": all_comp_dates, "datasets": []}
        
        if all_comp_dates:
            overall_daily = compl_df.groupby('Completed Date').size().reindex(all_comp_dates, fill_value=0)
            daily_data["datasets"].append({"label": "Overall", "values": [int(v) for v in overall_daily.values], "color": "#34c759"})
            course_daily = compl_df.groupby(['Completed Date', 'Content Name']).size().unstack(fill_value=0).reindex(all_comp_dates, fill_value=0)
            for course in course_daily.columns:
                course_values = [int(v) for v in course_daily[course].values]
                if sum(course_values) > 0: daily_data["datasets"].append({"label": course, "values": course_values})
        
        start_df = df[df['Start Date'] != '-'].copy()
        daily_start_chart = {"labels": [], "values": []}
        if not start_df.empty:
            user_first_start = start_df.groupby('Email')['Start Date'].min().reset_index()
            start_counts = user_first_start.groupby('Start Date').size().sort_index()
            daily_start_chart["labels"] = start_counts.index.tolist()
            daily_start_chart["values"] = [int(v) for v in start_counts.values]

        # Completion Days Stats
        calc_df = df[(df['Start Date'] != '-') & (df['Completed Date'] != '-')].copy()
        avg_days = median_days = mode_days = min_days = max_days = 0
        if not calc_df.empty:
            try:
                s_dt, c_dt = pd.to_datetime(calc_df['Start Date']), pd.to_datetime(calc_df['Completed Date'])
                diffs = (c_dt - s_dt).dt.days
                valid_diffs = diffs[diffs >= 0]
                if not valid_diffs.empty:
                    avg_days, median_days, min_days, max_days = round(valid_diffs.mean(), 1), round(valid_diffs.median(), 1), int(valid_diffs.min()), int(valid_diffs.max())
                    mode_series = valid_diffs.mode()
                    mode_days = int(mode_series.iloc[0]) if not mode_series.empty else 0
            except: avg_days = median_days = mode_days = min_days = max_days = "Error"

        u100, u51, u1, u0 = int((user_max >= 100).sum()), int(((user_max > 50) & (user_max < 100)).sum()), int(((user_max >= 1) & (user_max <= 50)).sum()), int((user_max < 1).sum())
        display_users = df.drop_duplicates('Email').to_dict(orient='records')
        
        c_stats = df.groupby(['Content Name', 'Content Provider']).agg(
            Total=('Email', 'count'),
            Completed=('Learning Status', lambda x: (x.astype(str) == 'Completed').sum()),
            InProgress=('Learning Status', lambda x: (x.astype(str) == 'In Progress').sum()),
            NotStarted=('Learning Status', lambda x: (x.astype(str) == 'Not Start').sum())
        ).reset_index().to_dict(orient='records')

        tier_order = ["0% (Not Start)", "1-10%", "11-20%", "21-30%", "31-40%", "41-50%", "51-60%", "61-70%", "71-80%", "81-90%", "91-99%", "100% (Complete)"]
        bins = [-1, 0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 99, 101]
        tier_labels = pd.cut(user_max, bins=bins, labels=tier_order)
        tier_counts = tier_labels.value_counts()
        chart_values = [int(tier_counts.get(t, 0)) for t in tier_order]

        daily_stats_list = []
        user_first_added = df.groupby('Email')['Added Date'].min()
        all_event_dates = sorted(list(set(df['Added Date'].unique().tolist() + df['Start Date'].unique().tolist() + df['Completed Date'].unique().tolist())))
        if '-' in all_event_dates: all_event_dates.remove('-')

        # New structure for the comparison line graph
        learning_comparison_chart = {
            "labels": all_event_dates,
            "datasets": [
                {"label": "Total Enrolled", "values": [], "color": "#0071e3"},
                {"label": "Not Started", "values": [], "color": "#86868b"},
                {"label": "Early (1-50%)", "values": [], "color": "#ff9500"},
                {"label": "Nearing (51-99%)", "values": [], "color": "#af52de"},
                {"label": "Completed", "values": [], "color": "#34c759"}
            ]
        }

        for d in all_event_dates:
            max_on_date = user_max_per_date[d] if d in user_max_per_date.columns else pd.Series([0]*len(user_max_per_date))
            
            # Calculate snapshots for each category on this date
            d_total = int((user_first_added <= d).sum())
            d_not_started = int((max_on_date < 1).sum()) if d in user_max_per_date.columns else 0
            d_early = int(((max_on_date >= 1) & (max_on_date <= 50)).sum()) if d in user_max_per_date.columns else 0
            d_nearing = int(((max_on_date > 50) & (max_on_date < 100)).sum()) if d in user_max_per_date.columns else 0
            d_completed = int((max_on_date >= 100).sum()) if d in user_max_per_date.columns else 0
            
            daily_stats_list.append({
                "date": d,
                "total": d_total,
                "not_started": d_not_started,
                "early": d_early,
                "nearing": d_nearing,
                "completed": d_completed
            })

            # Fill the chart datasets
            learning_comparison_chart["datasets"][0]["values"].append(d_total)
            learning_comparison_chart["datasets"][1]["values"].append(d_not_started)
            learning_comparison_chart["datasets"][2]["values"].append(d_early)
            learning_comparison_chart["datasets"][3]["values"].append(d_nearing)
            learning_comparison_chart["datasets"][4]["values"].append(d_completed)

        daily_stats_list.sort(key=lambda x: x['date'], reverse=True)

        comparison_chart = {"labels": all_event_dates, "added": [], "started": [], "completed": []}
        for d in all_event_dates:
            comparison_chart["added"].append(int((df['Added Date'] == d).sum()))
            comparison_chart["started"].append(int((df['Start Date'] == d).sum()))
            comparison_chart["completed"].append(int((df['Completed Date'] == d).sum()))

        # FB Ads vs Graduates comparison
        fb_grad_comparison = {"labels": [], "spend": [], "completions": []}
        # FB Ads vs Start Trend comparison
        fb_start_comparison = {"labels": [], "starts": [], "reach": [], "impressions": [], "cpr": [], "added": [], "completions": [], "campaign": []}
        
        all_dates_fb = sorted(list(set(all_event_dates + fb_df['Date'].tolist() + camp_df['Date'].tolist())))
        if '-' in all_dates_fb: all_dates_fb.remove('-')
        
        daily_compl_counts = df[df['Completed Date'] != '-'].groupby('Completed Date').size().to_dict()
        daily_added_counts = df.groupby('Added Date').size().to_dict()
        daily_camp_counts = camp_df.set_index('Date')['Count'].to_dict()
        
        # Calculate daily starts (count of users starting their first course)
        start_df = df[df['Start Date'] != '-'].copy()
        daily_start_counts = {}
        if not start_df.empty:
            daily_start_counts = start_df.groupby('Email')['Start Date'].min().value_counts().to_dict()
            
        daily_spend = fb_df.set_index('Date')['Amount spent (THB)'].to_dict()
        daily_reach = fb_df.set_index('Date')['Reach'].to_dict()
        daily_impressions = fb_df.set_index('Date')['Impressions'].to_dict()
        
        # Calculate Cost per Result (Spend / Starts) - although FB has its own, 
        # but user asked for comparison in one chart, let's provide FB's Reach/Imp
        
        for d in all_dates_fb:
            # For FB vs Grad
            fb_grad_comparison["labels"].append(d)
            s_val = daily_spend.get(d, 0)
            c_val = daily_compl_counts.get(d, 0)
            fb_grad_comparison["spend"].append(float(s_val) if pd.notna(s_val) else 0.0)
            fb_grad_comparison["completions"].append(int(c_val) if pd.notna(c_val) else 0)
            
            # For FB vs Starts
            fb_start_comparison["labels"].append(d)
            st_val = daily_start_counts.get(d, 0)
            reach_val = daily_reach.get(d, 0)
            imp_val = daily_impressions.get(d, 0)
            add_val = daily_added_counts.get(d, 0)
            comp_val = daily_compl_counts.get(d, 0)
            camp_val = daily_camp_counts.get(d, 0)
            
            fb_start_comparison["starts"].append(int(st_val))
            fb_start_comparison["reach"].append(int(reach_val))
            fb_start_comparison["impressions"].append(int(imp_val))
            fb_start_comparison["added"].append(int(add_val))
            fb_start_comparison["completions"].append(int(comp_val))
            fb_start_comparison["campaign"].append(int(camp_val))
            
            # Cost per result (Manual calculation based on starts for better insight)
            cpr = float(s_val) / int(st_val) if int(st_val) > 0 else 0
            fb_start_comparison["cpr"].append(round(cpr, 2))

        resp = make_response(render_template('summary.html', total_users=total_unique, joined_users_count=joined_count,
                               users_100_count=u100, users_51_99_count=u51, 
                               users_1_50_count=u1, users_0_count=u0, 
                               all_users_list=display_users, content_stats=c_stats,
                               latest_date=latest_date,
                               daily_completion_data=daily_data,
                               daily_start_data=daily_start_chart,
                               comparison_data=comparison_chart,
                               learning_comparison_data=learning_comparison_chart,
                               fb_grad_data=fb_grad_comparison,
                               fb_start_data=fb_start_comparison,
                               avg_days=avg_days, median_days=median_days, mode_days=mode_days, min_days=min_days, max_days=max_days,
                               chart_labels=tier_order, chart_values=chart_values, daily_stats=daily_stats_list,
                               learndi_stats=learndi_stats,
                               revenue_data=revenue_data,
                               grand_total_revenue=grand_total_revenue))
        resp.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
        return resp
    except Exception as e:
        traceback.print_exc()
        return f"Error: {e}", 500

@app.route('/report')
@login_required
def index():
    try:
        df = get_report_data()
        if df is None: return "Report not found."
        # Just send columns, let JS fetch the data
        resp = make_response(render_template('index.html', columns=df.columns.tolist(), data=[]))
        resp.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
        return resp
    except Exception as e: return f"Explorer Error: {e}", 500

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        u, p = request.form.get('username', '').strip(), request.form.get('password', '').strip()
        if USER_AUTH.get(u) == p:
            session['logged_in'], session['username'] = True, u
            return redirect(url_for('summary'))
        return render_template('login.html', error="Invalid credentials")
    return render_template('login.html')

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

@app.route('/api/refresh')
@login_required
def refresh_data():
    if refresh_logic(): return jsonify({"status": "success"})
    return jsonify({"status": "error"}), 500

@app.route('/api/report-data')
@login_required
def get_report_data_api():
    try:
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 100))
        search = request.args.get('search', '').lower()
        
        df = get_report_data()
        if df.empty:
            return jsonify({"data": [], "total_pages": 0, "current_page": page, "total_records": 0})
            
        # Apply search if provided
        if search:
            mask = df.apply(lambda x: x.astype(str).str.lower().str.contains(search).any(), axis=1)
            df = df[mask]
            
        total = len(df)
        total_pages = math.ceil(total / per_page)
        
        start = (page - 1) * per_page
        end = start + per_page
        paged_data = df.iloc[start:end].fillna('-').to_dict(orient='records')
        
        return jsonify({
            "data": paged_data,
            "total_pages": total_pages,
            "current_page": page,
            "total_records": total,
            "columns": df.columns.tolist()
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/calling-list')
@login_required
def calling_list():
    try:
        df_full = get_report_data()
        if df_full.empty or 'Email' not in df_full.columns:
            return render_template('calling_list.html', users=[], b2b_count=0, b2c_count=0, blank_count=0, s_pending=0, s_success=0, s_no_ans=0, s_not_int=0)
        
        df_calling = get_gas_calling_list()
        
        # เลือกเฉพาะคอลัมน์ที่จำเป็น ลดขนาด memory
        meta_cols = ['Email', 'First Name', 'Last Name', 'Content Name', 'Learning Status', 'User_Status_Category', 'Start Date', 'Completed Date']
        date_cols = [c for c in df_full.columns if c not in meta_cols and c not in ['Content Provider', 'date_joined', 'Added Date']]
        
        df = df_full[meta_cols + date_cols].copy()
        if date_cols:
            df['Row_Max_Prog'] = df[date_cols].apply(pd.to_numeric, errors='coerce').fillna(0).max(axis=1)
        else:
            df['Row_Max_Prog'] = 0

        # รวมกลุ่มข้อมูลด้วยวิธีที่เร็วขึ้น
        user_base = df.drop_duplicates('Email')[['Email', 'First Name', 'Last Name']]
        course_agg = df.groupby('Email')['Content Name'].unique().apply(lambda x: ' | '.join(sorted(x.astype(str)))).reset_index()
        
        # แยกดึงค่า max เพื่อความแม่นยำ
        max_vals = df.groupby('Email').agg({
            'Learning Status': lambda x: 'Completed' if 'Completed' in x.values else ('In Progress' if 'In Progress' in x.values else 'Not Start'),
            'Row_Max_Prog': 'max'
        }).reset_index()
        
        density = df.groupby('Email').size().reset_index(name='Density')

        user_summary = user_base.merge(course_agg, on='Email').merge(max_vals, on='Email').merge(density, on='Email')
        user_summary.rename(columns={'Row_Max_Prog': 'Max_Prog'}, inplace=True)

        def get_tier(p):
            if p >= 100: return "100%"
            if p <= 0: return "0%"
            tier = (int(p)//10)*10
            return f"{tier+1}-{tier+10}%"
        
        user_summary['User_Progress_Tier'] = user_summary['Max_Prog'].apply(get_tier)
        user_summary['Density_Display'] = user_summary['Density'].apply(lambda x: str(x) if x < 4 else "4+")
        user_summary['Email_Clean'] = user_summary['Email']

        if not df_calling.empty:
            df_calling_clean = df_calling.drop_duplicates('Email')[['Email', 'Phone', 'Status', 'Note', 'Customer Type']]
            user_summary = user_summary.merge(df_calling_clean, on='Email', how='left')
        else:
            for col in ['Phone', 'Status', 'Note', 'Customer Type']: user_summary[col] = "-"

        user_summary['Status'] = user_summary['Status'].fillna('Pending').replace(['-', ''], 'Pending')
        
        def clean_customer_type(x):
            s = str(x).strip().upper()
            if s == 'B2C': return 'B2C'
            if s == 'B2B': return 'B2B'
            return 'blank'
        user_summary['Customer Type'] = user_summary['Customer Type'].apply(clean_customer_type)
        
        b2b_count = int((user_summary['Customer Type'] == 'B2B').sum())
        b2c_count = int((user_summary['Customer Type'] == 'B2C').sum())
        blank_count = int((user_summary['Customer Type'] == 'blank').sum())
        status_counts = user_summary['Status'].value_counts()
        
        resp = make_response(render_template('calling_list.html', 
                               users=user_summary.fillna("-").to_dict(orient='records'),
                               b2b_count=b2b_count, b2c_count=b2c_count, blank_count=blank_count,
                               s_pending=int(status_counts.get('Pending', 0)),
                               s_success=int(status_counts.get('Call สำเร็จ', 0)),
                               s_no_ans=int(status_counts.get('ไม่มีคนรับสาย', 0)),
                               s_not_int=int(status_counts.get('ไม่สนใจเรียนต่อ', 0))))
        resp.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
        return resp
    except Exception as e:
        traceback.print_exc()
        return f"Calling List Error: {e}", 500

@app.route('/api/mark-called', methods=['POST'])
@login_required
def mark_called():
    global _cached_gas_df, _cached_gas_time
    data = request.json
    payload = {
        "action": "logCall", "email": data.get('email'), "firstName": data.get('fname'), "lastName": data.get('lname'),
        "phone": data.get('phone'), "status": data.get('status'), "customerType": data.get('customerType'),
        "note": data.get('note'), "progressTier": data.get('progressTier'), "username": session.get('username', 'Unknown'),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    try:
        resp = requests.post(GAS_URL, json=payload, timeout=15)
        if resp.status_code == 200: 
            # Clear cache upon success to force refresh on next load
            _cached_gas_df = None
            _cached_gas_time = None
            return jsonify(resp.json())
        return jsonify({"status": "error", "message": f"GAS Error: {resp.status_code}"}), 500
    except Exception as e: return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/sync-all-to-gsheet', methods=['POST'])
@login_required
def sync_all_to_gsheet():
    try:
        df_full = get_report_data()
        if df_full.empty or 'Email' not in df_full.columns:
            return jsonify({"status": "error", "message": "No data available to sync"}), 400
            
        # Meta columns
        meta_cols = ['Email', 'First Name', 'Last Name', 'Content Name', 'Learning Status']
        date_cols = [c for c in df_full.columns if c not in meta_cols and c not in ['Content Provider', 'date_joined', 'Added Date', 'Start Date', 'Completed Date', 'User_Status_Category']]
        
        # Prepare aggregated user data
        df = df_full.copy()
        if date_cols:
            df['Row_Max_Prog'] = df[date_cols].apply(pd.to_numeric, errors='coerce').fillna(0).max(axis=1)
        else:
            df['Row_Max_Prog'] = 0
            
        # Group by Email to get overall status and max progress
        user_agg = df.groupby('Email').agg({
            'First Name': 'first',
            'Last Name': 'first',
            'Learning Status': lambda x: 'Completed' if 'Completed' in x.values else ('In Progress' if 'In Progress' in x.values else 'Not Start'),
            'Row_Max_Prog': 'max'
        }).reset_index()
        
        density = df.groupby('Email').size().reset_index(name='Density')
        user_agg = user_agg.merge(density, on='Email')
        
        def get_tier(p):
            if p >= 100: return "100%"
            if p <= 0: return "0%"
            tier = (int(p)//10)*10
            return f"{tier+1}-{tier+10}%"
            
        user_agg['User_Progress_Tier'] = user_agg['Row_Max_Prog'].apply(get_tier)
        
        # Build batch users list for GAS
        batch_users = []
        for _, row in user_agg.iterrows():
            batch_users.append({
                "email": row['Email'],
                "firstName": row['First Name'],
                "lastName": row['Last Name'],
                "learningStatus": row['Learning Status'],
                "density": str(row['Density']),
                "progressTier": row['User_Progress_Tier']
            })
            
        payload = {
            "action": "batchAddUsers",
            "username": session.get('username', 'Unknown'),
            "users": batch_users
        }
        
        resp = requests.post(GAS_URL, json=payload, timeout=30)
        if resp.status_code == 200:
            return jsonify(resp.json())
        return jsonify({"status": "error", "message": f"GAS Error: {resp.status_code}"}), 500
    except Exception as e:
        traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)}), 500


def categorize_issue(row):
    # Combine description columns for scanning
    desc_cols = ['Description (รายละเอียดปัญหา)', 'Description Issue']
    text = ""
    for col in desc_cols:
        if col in row and pd.notna(row[col]):
            text += str(row[col]).lower() + " "
    
    # Define keywords for each category - Reordered and Refined
    categories = {
        "Certificate": ['ใบเซอร์', 'certificate', 'ประกาศนียบัตร', 'ดาวน์โหลด', 'ชื่อผิด', 'ยังไม่ได้', 'ได้รับใบ'],
        "Learning & Progress": ['เริ่มเรียน', 'เรียนไม่ได้', 'ปุ่มเทา', 'progress', 'ไม่ขึ้น', '100%', 'จบ', 'ไม่ผ่าน', 'ทำต่อไม่ได้', 'ไม่บันทึก', 'ปุ่มสีเทา', 'pre-test', 'คะแนนไม่แสดง'],
        "Video & Content": ['video', 'วิดีโอ', 'หมุน', 'คลิป', 'เนื้อหา', 'ไฟล์', 'pdf', 'เอกสาร', 'โหลดไม่ได้', 'เสียงไม่ดัง'],
        "Access & Login": ['login', 'เข้าไม่ได้', 'รหัสผ่าน', 'password', 'portal', 'สิทธิ์', 'หน้าขาว', 'error 403', 'portal ais', 'เข้าอบรม'],
        "Technical & Account": ['เปลี่ยนชื่อ', 'ข้อมูลผิด', 'ลบ', 'แก้ไข', 'ค้าง', 'ช้า', 'mobile', 'app', 'browser', 'เปลี่ยนอีเมล', 'cancel assignment']
    }
    
    for cat, keywords in categories.items():
        if any(kw in text for kw in keywords):
            return cat
            
    return "Other / General"

@app.route('/issue')
@login_required
def issue_page():
    try:
        resp = requests.get(ISSUE_SHEET_URL, timeout=15)
        if resp.status_code != 200: return "Error fetching issue data"
        
        with open(ISSUE_CSV, 'wb') as f: f.write(resp.content)
        df = pd.read_csv(ISSUE_CSV, low_memory=False)
        
        # Clean column names (strip whitespace and newlines)
        df.columns = [c.strip().replace('\n', ' ') for c in df.columns]
        
        # Filter: Must have Issue ID and Received Date (วันที่รับเรื่อง)
        date_col = "Received Date (วันที่รับเรื่อง)"
        id_col = "Issue ID"
        reporter_col = "Reporter Type (ประเภทผู้แจ้ง)"
        
        # Check if columns exist, if not try to find similar ones
        if date_col not in df.columns:
            for c in df.columns:
                if "Received Date" in c: date_col = c; break
        if id_col not in df.columns:
            for c in df.columns:
                if "Issue ID" in c: id_col = c; break
        if reporter_col not in df.columns:
            for c in df.columns:
                if "Reporter Type" in c: reporter_col = c; break

        # Helper to check if a value is "empty"
        def is_not_empty(val):
            s = str(val).strip()
            return s not in ['', '-', 'nan', 'None', '#N/A']

        # Filter rows: Must have both ID and Received Date
        df_valid = df[df[id_col].apply(is_not_empty) & df[date_col].apply(is_not_empty)].copy()
        
        # Apply Categorization
        df_valid['Category'] = df_valid.apply(categorize_issue, axis=1)
        
        # Count unique issues
        total_issues = int(df_valid[id_col].nunique())
        
        # Count by Reporter Type (based on unique Issue ID)
        # First, get unique issues with their reporter type
        unique_issues = df_valid.drop_duplicates(subset=[id_col])
        
        user_issues = int(unique_issues[unique_issues[reporter_col].astype(str).str.contains('User', case=False, na=False)].shape[0])
        admin_issues = int(unique_issues[unique_issues[reporter_col].astype(str).str.contains('Admin', case=False, na=False)].shape[0])
        other_issues = total_issues - (user_issues + admin_issues)

        # Category distribution for Pie Chart
        cat_counts = unique_issues['Category'].value_counts().to_dict()
        category_chart = {
            "labels": list(cat_counts.keys()),
            "values": [int(v) for v in cat_counts.values()]
        }
        category_summary = [{"name": k, "count": int(v)} for k, v in cat_counts.items()]

        # Prepare Chart Data
        chart_data = {"daily": {"labels": [], "values": []}, 
                      "weekly": {"labels": [], "values": []}, 
                      "monthly": {"labels": [], "values": []}}
        
        if not df_valid.empty:
            try:
                # Convert date column to datetime objects
                # We saw '25-Feb-2026' in the sample, which is %d-%b-%Y
                temp_df = df_valid.copy()
                temp_df['dt'] = pd.to_datetime(temp_df[date_col], errors='coerce', dayfirst=True)
                temp_df = temp_df.dropna(subset=['dt'])
                
                if not temp_df.empty:
                    # Daily
                    daily = temp_df.groupby(temp_df['dt'].dt.strftime('%Y-%m-%d')).size().sort_index()
                    chart_data["daily"]["labels"] = daily.index.tolist()
                    chart_data["daily"]["values"] = [int(v) for v in daily.values]
                    
                    # Weekly
                    # Using ISO week for consistency
                    weekly = temp_df.groupby(temp_df['dt'].dt.to_period('W').apply(lambda r: r.start_time.strftime('%Y-%m-%d'))).size().sort_index()
                    chart_data["weekly"]["labels"] = [str(i) for i in weekly.index.tolist()]
                    chart_data["weekly"]["values"] = [int(v) for v in weekly.values]
                    
                    # Monthly
                    monthly = temp_df.groupby(temp_df['dt'].dt.strftime('%Y-%m')).size().sort_index()
                    chart_data["monthly"]["labels"] = monthly.index.tolist()
                    chart_data["monthly"]["values"] = [int(v) for v in monthly.values]
            except Exception as de:
                print(f"Date processing error: {de}")

        # Prepare data for table (all valid records)
        display_data = df_valid.fillna('-').to_dict(orient='records')
        
        resp_page = make_response(render_template('issue.html', 
                                              total=total_issues, 
                                              user_count=user_issues, 
                                              admin_count=admin_issues,
                                              other_count=other_issues,
                                              issues=display_data,
                                              columns=['Category'] + [c for c in df_valid.columns if c != 'Category'],
                                              chart_data=chart_data,
                                              category_data=category_chart,
                                              category_summary=category_summary))
        resp_page.headers['Cache-Control'] = 'no-cache'
        return resp_page
    except Exception as e:
        traceback.print_exc()
        return f"Issue Page Error: {e}", 500

if __name__ == '__main__':
    load_auth_credentials()
    app.run(host='0.0.0.0', port=8080, debug=False)
