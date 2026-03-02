import os
import time
import json
import calendar
import requests
import pandas as pd
import gspread
from datetime import datetime
from zoneinfo import ZoneInfo
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials

# -------------------- START TIMER --------------------
start_time = time.time()

# -------------------- ENV & AUTH --------------------
sec = os.getenv("ASHRITHA_SECRET_KEY")
User_name = os.getenv("USERNAME")
service_account_json = os.getenv("SERVICE_ACCOUNT_JSON")
MB_URL = os.getenv("METABASE_URL")

if not sec or not service_account_json:
    raise ValueError("❌ Missing environment variables. Check GitHub secrets.")

# -------------------- GOOGLE AUTH --------------------
service_info = json.loads(service_account_json)
creds = Credentials.from_service_account_info(
    service_info,
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
)
gc = gspread.authorize(creds)

# -------------------- METABASE AUTH --------------------
res = requests.post(
    MB_URL,
    headers={"Content-Type": "application/json"},
    json={"username": User_name, "password": sec}
)
res.raise_for_status()
token = res.json()['id']
METABASE_HEADERS = {
    'Content-Type': 'application/json',
    'X-Metabase-Session': token
}
print("✅ Metabase session created")

# -------------------- SHEET KEYS --------------------
SHEET_BATCH_REPORT    = '1D-nPNTKoma5mS6B5_gAH5vRWysoGbE6inPghK969n8A'
SHEET_PROJECTS_MAIN   = '1e9BxI2N6ms1hh_OdfHgktntxi41_2Y2Jv5Oo-WnQDAo'
SHEET_EVALUATIONS     = '1gUshIBjLIVh4CqhVACVirqkPuHx6muCZIvXcXRY9qG4'
SHEET_LECTURE_SUBJ    = '1QKoJUTCM_rz7y0DVh_xEwqV1aGGd9dPHcZaLxBUtpiU'
SHEET_MENTOR          = '1XQRBzWqhn5DkEypVfCUJzdG5Y0P94oztPpXP4IscIVA'
SHEET_NPS             = '1fK-H9xyW9h0dnO97GHc0i07x7JHVhAgfclywKZ7bA-4'

# -------------------- UTILITIES --------------------
def mb_post(card_url):
    r = requests.post(card_url, headers=METABASE_HEADERS, timeout=120)
    r.raise_for_status()
    return r

def write_sheet(sheet_key, worksheet_name, df):
    print(f"🔄 Updating sheet: {worksheet_name}")
    for attempt in range(1, 6):
        try:
            sheet = gc.open_by_key(sheet_key)
            ws = sheet.worksheet(worksheet_name)
            ws.clear()
            set_with_dataframe(ws, df, include_index=False, include_column_header=True)
            print(f"✅ Successfully updated: {worksheet_name}")
            return
        except Exception as e:
            print(f"[Sheets] Attempt {attempt} failed for {worksheet_name}: {e}")
            if attempt < 5:
                time.sleep(20)
            else:
                print(f"❌ All attempts failed for {worksheet_name}.")
                raise

# -------------------- MONTH REPLACEMENTS --------------------
MONTH_REPLACEMENTS = {
    'Data Science Certification - December 2022': '2022 12 Dec',
    'Data Science Certification - January 2023': '2023 01 Jan',
    'Data Science Certification  - February 2023': '2023 02 Feb',
    'Data Science Certification  - March 2023': '2023 03 Mar',
    'Professional Certificate Course In Data Science - April 2023': '2023 04 April',
    'Professional Certificate Course In Data Science - September 2023': '2023 09 Sept',
    'Professional Certificate Course In Data Science - June 2023': '2023 06 June',
    'Professional Certificate Course In Data Science - July 2023': '2023 07 July',
    'Professional Certificate Course In Data Science - August 2023': '2023 08 Aug',
    'Professional Certificate Course In Data Science - May 2023': '2023 05 May',
    'Professional Certificate Course In Data Science - October 2023': '2023 10 Oct',
    'Professional Certificate Course In Data Science - November 2023': '2023 11 Nov',
    'Professional Certificate Course In Data Science - December 2023': '2023 12 Dec',
    'Professional Certificate Course In Data Science - January 2024': '2024 13 Jan',
    'Professional Certificate Course In Data Science - February 2024': '2024 14 Feb',
    'Professional Certificate Course In Data Science - March 2024': '2024 15 March',
    'Professional Certificate Course In Data Science - April 2024': '2024 16 April',
    'Professional Certificate Course In Data Science - May 2024': '2024 17 May',
    'Professional Certificate Course In Data Science - June 2024': '2024 18 June',
    'Professional Certificate Course In Data Science - July 2024': '2024 19 July',
    'Professional Certificate Course In Data Science - August 2024': '2024 20 Aug',
    'Professional Certificate Course In Data Science - September 2024': '2024 21 Sept',
    'Professional Certificate Course In Data Science - October 2024': '2024 22 Oct',
    'Certification in Advance Software Development October 2024': '2024 22 Oct ASD',
    'Professional Certificate Course In Data Science - November 2024': '2024 23 Nov',
    'Professional Certificate Course In Data Science - December 2024': '2024 24 Dec',
    'Professional Certificate Course In Data Science - January 2025': '2025 25 Jan',
    'Professional Certificate Course In Data Science - February 2025': '2025 26 Feb',
    'Professional Certificate Course In Data Science - March 2025': '2025 27 March',
    'Professional Certificate Course In Data Science - April 2025': '2025 28 April',
    'Professional Certificate Course In Data Science - May 2025': '2025 29 May',
    'Professional Certificate Course In Data Science & AI - June 2025': '2025 30 June',
    'Professional Certificate Course In Data Science - July 2025': '2025 31 July',
    'Professional Certificate Course In Data Science August 2025': '2025 32 August',
    'Professional Certificate Course In Data Science September 2025': '2025 33 September',
    'Professional Certificate Course In Data Science October 2025': '2025 34 October',
    'Professional Certificate Course In Data Science November 2025': '2025 35 November',
    'Professional Certificate Course In Data Science December 2025': '2025 36 December',
    'Professional Certificate Course In Data Science January 2026': '2026 37 January',
    'Professional Certificate Course In Data Science February 2026': '2026 38 Febraury',
    'DS Xcelerate AU': 'DS Xcelerate',
    'ASD Xcelerate AU': 'ASD Xcelerate'
}

def apply_month_replacements(series):
    def replace_month(match):
        return MONTH_REPLACEMENTS.get(match.group(0), match.group(0))
    pattern = '|'.join(map(re.escape, MONTH_REPLACEMENTS.keys()))
    return series.str.replace(pattern, replace_month, regex=True)

import re

# -------------------- SECTION 1: NPS --------------------
def run_nps():
    print("\n📌 Running: NPS")

    r = mb_post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/9452/query/json')
    df = pd.DataFrame(r.json())
    df = df.rename(columns={'course_name': 'Batch'})

    df['admin_unit_name'] = apply_month_replacements(df['admin_unit_name'])

    batches_to_retain = [
        '2025 25 Jan', '2025 26 Feb', '2025 27 March', '2025 28 April',
        '2025 29 May', '2025 30 June', '2025 31 July', '2025 32 August',
        '2025 33 September', '2025 34 October', '2025 35 November',
        '2025 36 December', '2026 37 January', '2026 38 Febraury'
    ]
    df = df[df['admin_unit_name'].isin(batches_to_retain)]
    df['form_fill_date'] = pd.to_datetime(df['form_fill_date'])

    def get_month_bucket(date):
        if pd.isnull(date):
            return None
        month_abbr = date.strftime('%b')
        year = date.year
        if date.day <= 15:
            return f"{month_abbr}-{year} (1-15)"
        else:
            last_day = calendar.monthrange(year, date.month)[1]
            return f"{month_abbr}-{year} (16-{last_day})"

    def categorize_nps(rating):
        if pd.isna(rating):
            return None
        try:
            rating = float(rating)
        except (ValueError, TypeError):
            return None
        if rating >= 9:
            return "Promoter"
        elif rating >= 7:
            return "Passive"
        else:
            return "Detractor"

    def determine_sentiment(current_category, previous_category):
        if pd.isna(current_category) or pd.isna(previous_category):
            return "No Change"
        if current_category == previous_category:
            return "No Change"
        transitions = {
            ("Promoter", "Passive"): "Promoter → Passive",
            ("Promoter", "Detractor"): "Promoter → Detractor",
            ("Passive", "Promoter"): "Passive → Promoter",
            ("Detractor", "Promoter"): "Detractor → Promoter",
            ("Passive", "Detractor"): "Passive → Detractor",
            ("Detractor", "Passive"): "Detractor → Passive",
        }
        return transitions.get((previous_category, current_category), "No Change")

    df['month_bucket'] = df['form_fill_date'].apply(get_month_bucket)
    df = df.sort_values(['user_id', 'form_fill_date'])
    df['current_nps_category'] = df['nps_rating'].apply(categorize_nps)
    df['previous_nps_rating'] = df.groupby('user_id')['nps_rating'].shift(1)
    df['previous_nps_category'] = df['previous_nps_rating'].apply(categorize_nps)
    df['sentiment'] = df.apply(
        lambda row: determine_sentiment(row['current_nps_category'], row['previous_nps_category']), axis=1
    )
    df.loc[df['previous_nps_category'].isna(), 'sentiment'] = 'No Previous'

    write_sheet(SHEET_NPS, "NPS_NEW", df)

# -------------------- SECTION 2: PROJECTS VIEW --------------------
def run_projects_view():
    print("\n📌 Running: Projects View")

    r1 = mb_post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/6959/query/json')
    r2 = mb_post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/6960/query/json')
    r3 = mb_post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/6289/query/json')

    df1 = pd.DataFrame(r1.json())
    df2 = pd.DataFrame(r2.json())
    df3 = pd.DataFrame(r3.json())[['user_id', 'au_batch_name', 'label']]
    df3 = df3[df3['label'] == 'Enrolled']

    concatenated_df = pd.concat([df1, df2], axis=0, ignore_index=True)
    concatenated_df = concatenated_df.drop(concatenated_df.index[0]).reset_index(drop=True)

    screened_df = concatenated_df[concatenated_df['User ID'].isin(df3['user_id'])]
    screened_df = screened_df.rename(columns={'User ID': 'user_id'})
    screened_df_1 = pd.merge(screened_df, df3, on='user_id')
    screened_df_1['au_batch_name'] = apply_month_replacements(screened_df_1['au_batch_name'])

    write_sheet(SHEET_PROJECTS_MAIN, "Projects_view", screened_df_1)

    # Also update Projects-1 (full concatenated with datetime cols)
    concatenated_df['Submission Time'] = pd.to_datetime(concatenated_df['Submission Time'])
    concatenated_df['latest_feedback_given_time'] = pd.to_datetime(concatenated_df['latest_feedback_given_time'])
    concatenated_df['project_deadline_date'] = pd.to_datetime(concatenated_df['project_deadline_date'])
    write_sheet(SHEET_PROJECTS_MAIN, "Projects-1", concatenated_df)

# -------------------- SECTION 3: LECTURES DATA --------------------
def run_lectures():
    print("\n📌 Running: Lectures Data")

    r1 = mb_post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/6031/query/json')
    r2 = mb_post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/8646/query/json')

    df  = pd.DataFrame(r1.json())
    df1 = pd.DataFrame(r2.json())

    df2 = pd.merge(df, df1, on=['lecture_id', 'lecture_date', 'course_name'], how='inner')
    df2 = df2.fillna(0)
    df2 = df2.dropna(subset=['module_name'])

    phase_2_modules = ['DS 05 Python', 'DS 06 EDA 1', 'DS 07 EDA 2']
    phase_3_modules = ['DS 08 ML 1 (old)', 'DS 09 ML 2', 'DS MLOPS']
    df2['module_group'] = df2['module_name'].apply(
        lambda x: 'Phase 2' if x in phase_2_modules
                  else 'Phase 3' if x in phase_3_modules
                  else x
    )
    df2 = df2.rename(columns={'course_name': 'Batch', 'module_name': 'Module'})
    write_sheet(SHEET_BATCH_REPORT, "Batch_Report", df2)

# -------------------- SECTION 4: ASSIGNMENT QUESTIONS BUCKET --------------------
def run_assignment_questions_bucket():
    print("\n📌 Running: Assignment Questions Bucket")

    r = mb_post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/7939/query/json')
    df = pd.DataFrame(r.json())
    df = df.fillna(0)
    df = df.rename(columns={'batch_name': 'Batch', 'module_name': 'Module'})
    write_sheet(SHEET_PROJECTS_MAIN, "Assignments_questions_bucket", df)

# -------------------- SECTION 5: PROJECTS RAW --------------------
def run_projects_raw():
    print("\n📌 Running: Projects Raw")

    r1 = mb_post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/6241/query/json')
    r2 = mb_post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/6242/query/json')
    r3 = mb_post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/6289/query/json')

    df1 = pd.DataFrame(r1.json())
    df2 = pd.DataFrame(r2.json())
    df3 = pd.DataFrame(r3.json())[['user_id', 'au_batch_name', 'label']]

    concatenated_df = pd.concat([df1, df2], axis=0, ignore_index=True)
    concatenated_df = concatenated_df.rename(columns={'User ID': 'user_id'})

    screened_df_1 = pd.merge(concatenated_df, df3, on='user_id', how='left')
    screened_df_1['Submission Time'] = pd.to_datetime(screened_df_1['Submission Time'])
    screened_df_1['latest_feedback_given_time'] = pd.to_datetime(screened_df_1['latest_feedback_given_time'])
    screened_df_1['project_deadline_date'] = pd.to_datetime(screened_df_1['project_deadline_date'])

    write_sheet(SHEET_PROJECTS_MAIN, "Projects", screened_df_1)

# -------------------- SECTION 6: PROJECT EVALUATIONS --------------------
def run_project_evaluations():
    print("\n📌 Running: Project Evaluations")

    r1 = mb_post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/6578/query/json')
    r2 = mb_post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/6579/query/json')

    df1 = pd.DataFrame(r1.json())
    df2 = pd.DataFrame(r2.json())
    df = pd.concat([df1, df2], axis=0, ignore_index=True)

    df['Submission Time'] = pd.to_datetime(df['Submission Time'])
    df['feedback_given_time'] = pd.to_datetime(df['feedback_given_time'])

    filtered_df = df[~(
        (df['feedback_given_time'].isnull() | (df['feedback_given_time'] == '')) &
        (df['Evaluation Status'] == 'Evaluated')
    )]
    filtered_df = filtered_df.drop_duplicates(subset='submission_id')
    filtered_df = filtered_df.rename(columns={'User ID': 'user_id'})

    write_sheet(SHEET_EVALUATIONS, "Project_evaluations", filtered_df)

# -------------------- SECTION 7: LECTURE QUALITY --------------------
def run_lecture_quality():
    print("\n📌 Running: Lecture Quality")

    r1 = mb_post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/8646/query/json')
    r2 = mb_post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/9192/query/json')
    r3 = mb_post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/9166/query/json')

    df1 = pd.DataFrame(r1.json())
    df2 = pd.DataFrame(r2.json())
    df3 = pd.merge(df1, df2, on=['lecture_id', 'lecture_date'], how='inner')
    df4 = df3.rename(columns={'course_name': 'Batch', 'lecture_date': 'date'})

    df_in_class = pd.DataFrame(r3.json())
    df_in_class = df_in_class.rename(columns={'batch_name': 'Batch', 'release_date': 'date'})

    df5 = pd.merge(df4, df_in_class, on=['Batch', 'date'], how='left')
    write_sheet(SHEET_BATCH_REPORT, "Lecture_Quality", df5)

# -------------------- SECTION 8: LECTURE SUBJECTIVE FEEDBACK --------------------
def run_lecture_subjective_feedback():
    print("\n📌 Running: Lecture Subjective Feedback")

    r = mb_post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/5037/query/json')
    df = pd.DataFrame(r.json())
    write_sheet(SHEET_LECTURE_SUBJ, "Lecture_Subjective_Feedback", df)

# -------------------- SECTION 9: MENTOR SESSIONS --------------------
def run_mentor_sessions():
    print("\n📌 Running: Mentor Sessions")

    r = mb_post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/6161/query/json')
    df = pd.DataFrame(r.json())
    df = df.rename(columns={'batch': 'Batch'})
    df['week_view'] = pd.to_datetime(df['week_view'])
    write_sheet(SHEET_MENTOR, "Mentor_sessions", df)

# -------------------- SECTION 10: MENTOR GROUP SESSIONS --------------------
def run_mentor_group_sessions():
    print("\n📌 Running: Mentor Group Sessions")

    r = mb_post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/6184/query/json')
    df = pd.DataFrame(r.json())
    df = df.rename(columns={'batch': 'Batch', 'Mentor Name': 'mentor_name'})
    write_sheet(SHEET_MENTOR, "Mentor_group_sessions", df)

# -------------------- SECTION 11: MENTOR SLOTS + MENTOR BATCH --------------------
def run_mentor_slots_and_batch():
    print("\n📌 Running: Mentor Slots & Mentor-Batch")

    r3 = mb_post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/7019/query/json')
    df3 = pd.DataFrame(r3.json())
    df3['date'] = pd.to_datetime(df3['date'])
    write_sheet(SHEET_MENTOR, "Mentor_slots", df3)

    r6 = mb_post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/7941/query/json')
    df6 = pd.DataFrame(r6.json())
    df5 = pd.merge(df6, df3, on=['mentor_id', 'mentor_name'], how='left')
    df5 = df5.fillna(0)
    write_sheet(SHEET_MENTOR, "Mentor-Batch", df5)

# -------------------- SECTION 12: MENTOR CSAT --------------------
def run_mentor_csat():
    print("\n📌 Running: Mentor CSAT")

    r = mb_post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/6167/query/json')
    df = pd.DataFrame(r.json())
    df = df.rename(columns={'batch': 'Batch', 'Mentor Name': 'mentor_name'})
    write_sheet(SHEET_MENTOR, "Mentor_CSAT", df)

# -------------------- MAIN: RUN ALL --------------------
if __name__ == "__main__":
    print("🚀 Starting Data Pipeline Automation...")

    tasks = [
        ("NPS",                         run_nps),
        ("Projects View",               run_projects_view),
        ("Lectures Data",               run_lectures),
        ("Assignment Questions Bucket", run_assignment_questions_bucket),
        ("Projects Raw",                run_projects_raw),
        ("Project Evaluations",         run_project_evaluations),
        ("Lecture Quality",             run_lecture_quality),
        ("Lecture Subjective Feedback", run_lecture_subjective_feedback),
        ("Mentor Sessions",             run_mentor_sessions),
        ("Mentor Group Sessions",       run_mentor_group_sessions),
        ("Mentor Slots & Batch",        run_mentor_slots_and_batch),
        ("Mentor CSAT",                 run_mentor_csat),
    ]

    for name, fn in tasks:
        try:
            fn()
        except Exception as e:
            print(f"❌ Error in {name}: {e}")

    current_time = datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%d-%b-%Y %H:%M:%S")
    print(f"\n✅ Timestamp: {current_time}")

    end_time = time.time()
    mins, secs = divmod(end_time - start_time, 60)
    print(f"⏱ Total time: {int(mins)}m {int(secs)}s")
    print("🎯 Data Pipeline Automation completed successfully!")