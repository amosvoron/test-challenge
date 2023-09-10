import csv
from datetime import datetime, timedelta
from collections import Counter
import itertools

# Param
CONSECUTIVE_DAYS = 15

# Reads CSV content and returns a list of tuples
# each tuple representing 1 row from the CSV file
def read_csv(path):
    with open(path, newline='') as f:
        reader = csv.reader(f)
        data = [tuple(row) for row in reader]
    return data[1:]  # exclude header

# Read from CSV and remove header
# Remark on data optimization:
# --------------------------------------------------------------
# Since the data is processed using numeric identifiers,
# the students and tests are redundant datasets. So, we will
# only load submissions and grades.
# --------------------------------------------------------------
submissions = read_csv('submissions.csv') 
grades = read_csv('grades.csv') 

# Append new columns and apply casting to provide correct data types:
data = [(int(x[0]),                                    
         int(x[1]),                                     
         int(x[2]),                                    
         datetime.strptime(x[3], '%Y-%m-%d %H:%M:%S'),  
         datetime.strptime(x[3][:10], '%Y-%m-%d'))      
        for x in submissions]

# Validate
if len(data) == 0:
    raise Exception('At least one CSV file is empty. Processing interrupted. Please provide non-empty CSV files.')

# Convert grades to dictionary
grades = dict([(int(row[0]), 
                int(row[1])) for row in grades])

# Append grade to submissions -> data:
# --------------------------------------
#   data[0] -> submission_id
#   data[1] -> test_id
#   data[2] -> student_id
#   data[3] -> submission_time
#   data[4] -> submission_date
#   data[5] -> grade
# --------------------------------------
# Note: If a grade does not exist, the default 0 value is assigned.
data = [(x[0], x[1], x[2], x[3], x[4], grades.get(x[0], 0)) for x in data]

# Sort data by submission_date descending
data.sort(key=lambda x: x[4], reverse=True)

# Calculate submission_start to filter data by last CONSECUTIVE_DAYS
submission_start = data[0][4] - timedelta(days=CONSECUTIVE_DAYS-1)

# --------------------
# Query 1 
# --------------------
# Solution design:
#  - Filter by consecutive days
#  - Select distinct (student_id, submission_date) rows 
#  - Extract only those students whose submission count match CONSECUTIVE_DAYS
data1 = list(set([(x[2], x[4]) for x in data if x[4] >= submission_start]))
counter1 = Counter([x[0] for x in data1])
count1 = 0
for student_id in counter1:
    if counter1[student_id] == CONSECUTIVE_DAYS:
        count1 += 1
print(f'\nResult #1: {count1}')

# --------------------
# Query 2
# --------------------
# Solution design:
#  - Filter by consecutive days AND grade > 0 
#  - Select distinct (student_id, submission_date) rows 
#  - Extract only those students whose submission count match CONSECUTIVE_DAYS
data2 = list(set([(x[2], x[4]) for x in data if (x[4] >= submission_start and x[5] > 0)]))
counter2 = Counter([x[0] for x in data2])
count2 = 0
for student_id in counter2:
    if counter2[student_id] == CONSECUTIVE_DAYS:
        count2 += 1
print(f'\nResult #1: {count2}')

# --------------------
# Query 3
# --------------------
# Solution design:
#  - Filter by consecutive days (use already filtered data1)
#  - Select distinct (student_id, submission_date) rows 
#  - Extract only those students whose submission count match CONSECUTIVE_DAYS
data3 = [(x[2], x[4]) for x in data if (x[4] >= submission_start)]
counter3 = Counter([x for x in data3])

# Sort list of (submission_date, student_id, count) by submission_date ASC, 
# student_id ASC, count DESC
# Note using -x[1] to achieve the reverse order of count
data3_sorted = []
for x in counter3:
    data3_sorted.append((x[1], x[0], counter3[x]))
data3_sorted.sort(key=lambda x: (x[0], -x[2], x[1]), reverse=False)

# For each submission_date, select first row from the sorted list
result3 = []
prev_submission_date = None
for x in data3_sorted:
    submission_date, student_id, count = x
    if (prev_submission_date is None) or (submission_date > prev_submission_date):
        result3.append((submission_date, student_id, count))
        prev_submission_date = submission_date
        
print(f'\nResult #3:')
print('------------------------------------')
print('submission_date | student_id | count')
print('------------------------------------')
for row in result3:
    print("{} | {} | {}".format(row[0].strftime('%Y-%m-%d').ljust(15), 
                                str(row[1]).rjust(10),
                                str(row[2]).rjust(5)))
print('------------------------------------')        

# --------------------
# Query 4
# --------------------
# Solution design:
#   1. Calculate the total grade per (student_id, test_id) -> needed in the next step.
#   2. Split data into 2 subsets:
#      - invalid_submissions: all submissions per (student_id, test_id) are invalid (grade=0) 
#        -> take distinct (student_id, test_id) rows with grade=0
#      - last_valid_submission: at least one submission is valid
#        -> take last valid submission per (student_id, test_id)
#   3. Union both subsets and compute the grade average per test_id.

# Calculate total grade per (test, student) group and store them into a dictionary
data4 = [(x[1], x[2], x[5]) for x in data]
data4.sort(key=lambda x: (x[0], x[1]))
total_grades = {(test_id, student_id): sum(item[2] for item in group) for (test_id, student_id), group 
                in itertools.groupby(data4, key=lambda x: (x[0], x[1]))}

# Process invalid submissions
invalid_submissions = list(set([x for x in data4 if total_grades[(x[0], x[1])] == 0]))

# Extract valid submissions
valid_submissions = [(x[1], x[2], x[3], x[5]) for x in data 
                        if (total_grades[(x[1], x[2])] > 0) and (x[5] > 0)]

# Sort the list by date in descending order
valid_submissions.sort(key=lambda x: x[2], reverse=True)

# Extract only most recent submissions
valid_submissions_extract = []
seen = set()
for x in valid_submissions:
    test_id, student_id, _, grade = x
    if (test_id, student_id) not in seen:
        seen.add((test_id, student_id))
        valid_submissions_extract.append((test_id, student_id, grade))
        
# Create a union of both lists
union_submissions = valid_submissions_extract + invalid_submissions
        
# Compute average 
union_submissions.sort(key=lambda x: x[0])
result4 = []
for test_id, group in itertools.groupby(union_submissions, key=lambda x: x[0]):
    grades = [item[2] for item in group]
    result4.append((test_id, sum(grades) / len(grades)))

# Output
print(f'\nResult #4:')
print('-------------------')
print('test_id | avg_grade')
print('-------------------')
for row in result4:
    print("{} | {:.2f}".format(str(row[0]).rjust(7),
                               round(row[1], 2)))
print('-------------------')



