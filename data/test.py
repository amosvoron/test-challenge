import numpy as np
import pandas as pd

# -------------------------
#  Parameters
# -------------------------

# Number of consecutive days during which the student 
# must have submitted at least 1 test 
CONSECUTIVE_DAYS = 15

# -------------------------
#  Load & Transform
# -------------------------

students = pd.read_csv('students.csv')
submissions = pd.read_csv('submissions.csv')
tests = pd.read_csv('tests.csv')
grades = pd.read_csv('grades.csv')

# Merge all dataframes (and remove unused columns)
df = students \
    .merge(submissions, on='student_id') \
    .merge(tests, on='test_id') \
    .merge(grades, on='submission_id', how='left') \
    .drop(['student_name', 'test_name'], axis=1)

# Ensure the submission_time is datetime and append submission date
df['submission_time'] = pd.to_datetime(df['submission_time'])
df['submission_date'] = df['submission_time'].apply(lambda x: x.date())

# Sort df by submission time
df = df.sort_values(by='submission_time')

# -------------------------
#  Validate
# -------------------------

if df.shape[0] == 0:
    raise Exception('At least one CSV file is empty. Processing interrupted. Please provide non-empty CSV files.')

# -------------------------
#  Common Functions
# -------------------------

# Extracts CONSECUTIVE_DAYS of data
def extract_consecutive_days(df):
    return df[(df['submission_time'] > (df['submission_time'].max() - pd.Timedelta(days=CONSECUTIVE_DAYS)))]

# Select only those students that satisfy the condition of consecutive days
def filter_students(filtered):
    result = filtered.groupby('student_id').nunique() == CONSECUTIVE_DAYS
    result = result.rename(columns={'submission_date': 'is_consecutive'})
    return result[result['is_consecutive']].shape[0]

# ----------------------------------------------------------------------------------
#  Query 1
#
#  Calculate the number of unique students who submitted at least 1 test each day, 
#  for the last 15 days of data available.
# ----------------------------------------------------------------------------------

# Pass unique combinations of all (student, submission) rows to the filter
extracted1 = extract_consecutive_days(df)
filtered1 = extracted1[['student_id', 'submission_date']].drop_duplicates()
result1 = filter_students(filtered1)
print(f'\nResult #1: {result1}')

# ----------------------------------------------------------------------------------
#  Query 2
#
#  The number of unique students who submitted at least 1 valid test each day, 
#  for the last 15 days of data available.
# ----------------------------------------------------------------------------------

# Pass unique combinations of all valid (student, submission) rows to the filter, grade > 0.
filtered2 = extracted1[extracted1.grade > 0][['student_id', 'submission_date']].drop_duplicates()
result2 = filter_students(filtered2)
print(f'\nResult #2: {result2}')

# ----------------------------------------------------------------------------------
#  Query 3
#
#  The student with the most submissions for each day of the last 15 days of data 
#  available. If there are two or more students with the same results, print the 
#  student with the lowest ID.
# ----------------------------------------------------------------------------------

# Calculate the count of rows per (student, submission_date) -> submission_count
students_submission_count = extracted1 \
    .groupby(['student_id', 'submission_date']) \
    .size() \
    .reset_index(name='submission_count')

# For each submission_date, get the student with the highest submission count 
# (and the lowest student_id in case of ties)
result3 = students_submission_count \
    .sort_values(by=['submission_date', 'submission_count', 'student_id'], 
                 ascending=[True, False, True]) \
    .groupby('submission_date') \
    .first() 

print(f'\nResult #3:')
print(result3)

# ----------------------------------------------------------------------------------
#  Query 3
#
#  The average grade for each test. If a student has submitted just once, consider 
#  that grade regardless of its value. If the student submitted the same test 
#  multiple times, give preference to the last valid grade.
# 
#  Instruction analysis with remark on detected ambiguity:
#    A: Calculate the average grade per test.
#    B: A single submission of (student, test) => take it (even if invalid)
#    C: Multiple submission of (student, test) => take the last valid submission
#
#  What about the case when all multiple submission are invalid? 
#  Since we cannot find any valid submission, the subset (student, test) is empty.
#  Then, on one hand, we allow that the invalid submission is accepted if only 
#  one per (student, test), on the other hand, we reject all multiple submissions 
#  if they are invalid.
#
#  To overcome this ambiguity a new rule has been added:
#     D: If all multiple submissions of (student, test) are invalid, 
#        take any submission.
# ----------------------------------------------------------------------------------

# Logic to apply to all members of a group (student_id, test_id)
def get_relevant_submission(student_test_df):
    valid_student_test_df = student_test_df[student_test_df['grade'] > 0]
    # If the filtered (valid) group is not empty, take the last member 
    if not valid_student_test_df.empty:
        return valid_student_test_df.iloc[-1]
    # Otherwise, take the last member of the unfiltered group 
    else:
        return student_test_df.iloc[-1]

# Apply the logic to get the selected submission for each (student_id, test_id) combination
selected_submissions = df \
    .groupby(['student_id', 'test_id']) \
    .apply(get_relevant_submission) \
    .reset_index(drop=True)

# Compute the average grade per test
result4 = selected_submissions \
    .groupby('test_id')['grade'] \
    .mean() \
    .reset_index() \
    .rename(columns={'grade': 'avg_grade'})

print(f'\nResult #4:')
print(result4)
