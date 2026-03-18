from flask import Flask, render_template, jsonify, url_for, request, redirect, session
import pandas as pd
import numpy as np
import os
import requests
from functools import wraps
import sys
import traceback
from datetime import datetime

app = Flask(__name__, 
            static_folder="../static", 
            template_folder="../templates")
application = app # Add this for Vercel WSGI entry point
app.secret_key = "conicle_secret_key_for_session"

# URLs
DATA_SHEET_URL = "https://docs.google.com/spreadsheets/d/18YTiZ67OCVvTXltashgTPIvpH6u8e6GnAb986PctVOI/export?format=csv&gid=0"
AUTH_SHEET_URL = "https://docs.google.com/spreadsheets/d/18YTiZ67OCVvTXltashgTPIvpH6u8e6GnAb986PctVOI/export?format=csv&gid=56353457"
USER_ACCESS_URL = "https://docs.google.com/spreadsheets/d/18YTiZ67OCVvTXltashgTPIvpH6u8e6GnAb986PctVOI/export?format=csv&gid=94070811"
CALLING_LIST_URL = "https://docs.google.com/spreadsheets/d/18YTiZ67OCVvTXltashgTPIvpH6u8e6GnAb986PctVOI/export?format=csv&gid=2097893051"

# Paths
REPORT_CSV = "/tmp/STEM_Learning_Report.csv"
REMOTE_CSV = "/tmp/googlesheet_report.csv"
AUTH_CSV = "/tmp/auth_users.csv"
USER_ACCESS_CSV = "/tmp/user_access.csv"
CALLING_LIST_CSV = "/tmp/calling_list.csv"

# Google Apps Script Web App URL (Update this with your actual URL)
# Example: https://script.google.com/macros/s/AKfycb.../exec
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
        resp = requests.get(AUTH_SHEET_URL, timeout=10)
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
            if 'email' in df.columns and 'date_joined' in df.columns:
                df['email'] = df['email'].astype(str).str.strip().str.lower()
                return df[['email', 'date_joined']].drop_duplicates('email')
    except Exception as e: print(f"User access load error: {e}")
    return pd.DataFrame(columns=['email', 'date_joined'])

def process_data():
    if not os.path.exists(REMOTE_CSV): return False
    try:
        df_raw = pd.read_csv(REMOTE_CSV, low_memory=False)
        df_access = load_user_access_data()
        df_raw['Email'] = df_raw['Email'].astype(str).str.strip().str.lower()
        df_raw['Progress Percentage'] = pd.to_numeric(df_raw['Progress Percentage'], errors='coerce').fillna(0)
        
        if not df_access.empty:
            df_raw = df_raw.merge(df_access, left_on='Email', right_on='email', how='left').drop(columns=['email'])
        else:
            df_raw['date_joined'] = np.nan

        df_raw['Content Provider'] = df_raw['Content Provider'].fillna('Unknown')
        df_raw['Date_Parsed'] = pd.to_datetime(df_raw['Date'], errors='coerce')
        df_raw['Date_Str'] = df_raw['Date_Parsed'].dt.strftime('%Y-%m-%d')
        
        df_sorted = df_raw.sort_values(['Email', 'Content Name', 'Date_Parsed'], ascending=[True, True, False])
        latest_status = df_sorted.drop_duplicates(['Email', 'Content Name'])[['Email', 'Content Name', 'Transaction Status']]
        valid_enrollments = latest_status[latest_status['Transaction Status'] == 'Learner']
        df = df_raw.merge(valid_enrollments[['Email', 'Content Name']], on=['Email', 'Content Name'], how='inner').copy()
        
        index_cols = ['Email', 'First Name', 'Last Name', 'Content Name', 'Content Provider', 'date_joined']
        for col in index_cols: df[col] = df[col].fillna('-')

        pivot_df = df.pivot_table(index=index_cols, columns='Date_Str', values='Progress Percentage', aggfunc='max').reset_index()
        pivot_df.columns.name = None
        
        date_cols = sorted([col for col in pivot_df.columns if col not in index_cols])
        
        def determine_status(row):
            vals = pd.to_numeric(row[date_cols], errors='coerce').dropna()
            if vals.empty: return 'Not Start'
            last_val = vals.iloc[-1]
            if last_val >= 100: return 'Completed'
            if last_val > 0: return 'In Progress'
            return 'Not Start'
        
        pivot_df['Learning Status'] = pivot_df.apply(determine_status, axis=1)

        # Add User_Status_Category for filtering in UI
        def get_user_cat(row):
            vals = pd.to_numeric(row[date_cols], errors='coerce').dropna()
            if vals.empty: return '0'
            m = vals.max()
            if m >= 100: return '100'
            if m > 50: return 'in-progress-high'
            if m >= 1: return 'in-progress-early'
            return '0'
        
        pivot_df['User_Status_Category'] = pivot_df.apply(get_user_cat, axis=1)

        pivot_df.to_csv(REPORT_CSV, index=False, encoding='utf-8-sig')
        return True
    except Exception as e: 
        print(f"Process error: {e}")
        return False

def get_report_data():
    if not os.path.exists(REPORT_CSV): refresh_logic()
    if os.path.exists(REPORT_CSV): return pd.read_csv(REPORT_CSV, low_memory=False)
    return None

def refresh_logic():
    try:
        if os.path.exists(REPORT_CSV): os.remove(REPORT_CSV)
        r1 = requests.get(DATA_SHEET_URL, timeout=15)
        if r1.status_code == 200:
            with open(REMOTE_CSV, 'wb') as f:
                f.write(r1.content)
        r2 = requests.get(USER_ACCESS_URL, timeout=15)
        if r2.status_code == 200:
            with open(USER_ACCESS_CSV, 'wb') as f:
                f.write(r2.content)
        load_auth_credentials()
        return process_data()
    except Exception as e: 
        return False

@app.route('/')
@login_required
def summary():
    try:
        df = get_report_data()
        if df is None: return "Database Error"
        
        meta = ['Email', 'First Name', 'Last Name', 'Content Name', 'Content Provider', 'date_joined', 'Learning Status', 'User_Status_Category']
        date_cols = [c for c in df.columns if c not in meta]
        latest_date = date_cols[-1] if date_cols else None
        
        def find_completion_date(row):
            for d in date_cols:
                if pd.to_numeric(row[d], errors='coerce') >= 100:
                    return d
            return None
        
        df['Completion_Date'] = df.apply(find_completion_date, axis=1)
        daily_completions = df[df['Completion_Date'].notnull()].groupby('Completion_Date').size().sort_index().reset_index(name='count')
        
        daily_data = {
            "labels": daily_completions['Completion_Date'].tolist(),
            "values": daily_completions['count'].tolist()
        }
        
        total_unique = df['Email'].nunique()
        joined_count = df[df['date_joined'].astype(str) != '-']['Email'].nunique()

        if date_cols:
            user_max = df.groupby('Email')[date_cols].apply(lambda x: x.apply(pd.to_numeric, errors='coerce').fillna(0).max()).max(axis=1)
        else:
            user_max = pd.Series(0, index=df['Email'].unique())
        
        u100 = int((user_max >= 100).sum())
        u51 = int(((user_max > 50) & (user_max < 100)).sum())
        u1 = int(((user_max >= 1) & (user_max <= 50)).sum())
        u0 = int((user_max < 1).sum())

        display_users = df.drop_duplicates('Email').to_dict(orient='records')
        
        c_stats = df.groupby(['Content Name', 'Content Provider']).agg(
            Total=('Email', 'count'),
            Completed=('Learning Status', lambda x: (x.astype(str) == 'Completed').sum()),
            InProgress=('Learning Status', lambda x: (x.astype(str) == 'In Progress').sum()),
            NotStarted=('Learning Status', lambda x: (x.astype(str) == 'Not Start').sum())
        ).reset_index().to_dict(orient='records')

        # Density Chart Data
        density = df.groupby('Email').size().value_counts().sort_index()
        chart_labels = [f"{i} Courses" for i in density.index]
        chart_values = density.values.tolist()

        return render_template('summary.html', total_users=total_unique, joined_users_count=joined_count,
                               users_100_count=u100, users_51_99_count=u51, 
                               users_1_50_count=u1, users_0_count=u0, 
                               all_users_list=display_users, content_stats=c_stats,
                               daily_completion_data=daily_data,
                               latest_date=latest_date,
                               chart_labels=chart_labels,
                               chart_values=chart_values)
    except Exception as e:
        traceback.print_exc()
        return f"Error: {e}", 500

@app.route('/report')
@login_required
def index():
    try:
        df = get_report_data()
        if df is None: return "Report not found."
        return render_template('index.html', columns=df.columns.tolist(), data=df.fillna('-').to_dict(orient='records'))
    except Exception as e:
        return f"Explorer Error: {e}", 500

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

@app.route('/calling-list')
@login_required
def calling_list():
    try:
        df = get_report_data()
        if df is None: return "Report not found."
        
        # Load existing calling list from sheet (via GAS JSON API)
        df_calling = pd.DataFrame() # Initialize as empty
        try:
            if not GAS_URL or "placeholder" in GAS_URL.lower():
                print("GAS_URL is not configured for fetching calling list.")
            else:
                resp = requests.get(f"{GAS_URL}?action=getCallingList", timeout=10)
                if resp.status_code == 200:
                    gas_data = resp.json()
                    if gas_data and isinstance(gas_data, list):
                        df_calling = pd.DataFrame(gas_data)
                        df_calling.columns = [c.strip() for c in df_calling.columns]
                        if 'Email' in df_calling.columns:
                            df_calling['Email'] = df_calling['Email'].astype(str).str.strip().str.lower()
                    else:
                        print(f"GAS returned empty or invalid data: {gas_data}")
                else:
                    print(f"Failed to fetch calling list from GAS. Status: {resp.status_code}")
        except Exception as e:
            print(f"Error fetching calling list from GAS: {e}")

        # Process main data for density and progress
        meta = ['Email', 'First Name', 'Last Name', 'Content Name', 'Content Provider', 'date_joined', 'Learning Status', 'User_Status_Category']
        date_cols = [c for c in df.columns if c not in meta]
        
        user_summary = df.groupby('Email').agg({
            'First Name': 'first',
            'Last Name': 'first',
            'Learning Status': lambda x: 'Completed' if 'Completed' in x.values else ('In Progress' if 'In Progress' in x.values else 'Not Start')
        }).reset_index()

        density = df.groupby('Email').size().reset_index(name='Density')
        user_summary = user_summary.merge(density, on='Email')
        
        if date_cols:
            df['Max_Prog'] = df[date_cols].apply(pd.to_numeric, errors='coerce').fillna(0).max(axis=1)
            user_prog = df.groupby('Email')['Max_Prog'].max().reset_index()
            user_summary = user_summary.merge(user_prog, on='Email')
        else:
            user_summary['Max_Prog'] = 0

        def get_tier(p):
            if p >= 100: return "100%"
            if p <= 0: return "0%"
            tier = (int(p)//10)*10
            return f"{tier+1}-{tier+10}%"
        
        user_summary['User_Progress_Tier'] = user_summary['Max_Prog'].apply(get_tier)
        user_summary['Density_Display'] = user_summary['Density'].apply(lambda x: str(x) if x < 4 else "4+")
        user_summary['Email_Clean'] = user_summary['Email']

        # Merge with Calling List (Status, Note, Phone, Type)
        if not df_calling.empty:
            df_calling = df_calling.drop_duplicates('Email')
            user_summary = user_summary.merge(df_calling[['Email', 'Phone', 'Status', 'Note', 'Customer Type']], on='Email', how='left')
        else:
            for col in ['Phone', 'Status', 'Note', 'Customer Type']: user_summary[col] = "-"

        return render_template('calling_list.html', users=user_summary.fillna("-").to_dict(orient='records'))
    except Exception as e:
        traceback.print_exc()
        return f"Calling List Error: {e}", 500

@app.route('/api/mark-called', methods=['POST'])
@login_required
def mark_called():
    data = request.json
    payload = {
        "action": "logCall",
        "email": data.get('email'),
        "firstName": data.get('fname'),
        "lastName": data.get('lname'),
        "phone": data.get('phone'),
        "status": data.get('status'),
        "customerType": data.get('customerType'),
        "note": data.get('note'),
        "progressTier": data.get('progressTier'),
        "username": session.get('username', 'Unknown'),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    try:
        if not GAS_URL or "placeholder" in GAS_URL.lower():
            return jsonify({"status": "error", "message": "GAS_URL is not configured"}), 400
            
        resp = requests.post(GAS_URL, json=payload, timeout=15)
        if resp.status_code == 200:
            return jsonify(resp.json())
        return jsonify({"status": "error", "message": f"GAS Error: {resp.status_code}"}), 500
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/sync-all-to-gsheet', methods=['POST'])
@login_required
def sync_all_to_gsheet():
    try:
        df = get_report_data()
        if df is None: return jsonify({"status": "error", "message": "No data to sync"}), 500
        
        # Prepare batch users
        meta = ['Email', 'First Name', 'Last Name', 'Content Name', 'Content Provider', 'date_joined', 'Learning Status', 'User_Status_Category']
        date_cols = [c for c in df.columns if c not in meta]
        
        user_summary = df.groupby('Email').agg({
            'First Name': 'first',
            'Last Name': 'first',
            'Learning Status': lambda x: 'Completed' if 'Completed' in x.values else ('In Progress' if 'In Progress' in x.values else 'Not Start')
        }).reset_index()

        density = df.groupby('Email').size().reset_index(name='Density')
        user_summary = user_summary.merge(density, on='Email')
        
        if date_cols:
            df['Max_Prog'] = df[date_cols].apply(pd.to_numeric, errors='coerce').fillna(0).max(axis=1)
            user_prog = df.groupby('Email')['Max_Prog'].max().reset_index()
            user_summary = user_summary.merge(user_prog, on='Email')
        else:
            user_summary['Max_Prog'] = 0

        def get_tier(p):
            if p >= 100: return "100%"
            if p <= 0: return "0%"
            tier = (int(p)//10)*10
            return f"{tier+1}-{tier+10}%"
        
        user_summary['User_Progress_Tier'] = user_summary['Max_Prog'].apply(get_tier)
        
        batch_users = []
        for _, row in user_summary.iterrows():
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
            "users": batch_users,
            "username": session.get('username', 'System')
        }
        
        resp = requests.post(GAS_URL, json=payload, timeout=30)
        if resp.status_code == 200:
            return jsonify(resp.json())
        return jsonify({"status": "error", "message": f"GAS Error: {resp.status_code}"}), 500
    except Exception as e:
        traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    load_auth_credentials()
    app.run(host='0.0.0.0', port=8080, debug=False)
