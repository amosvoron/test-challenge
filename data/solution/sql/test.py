import duckdb

# Load from CSVs
students = duckdb.read_csv('students.csv') 
tests = duckdb.read_csv('tests.csv') 
submissions = duckdb.read_csv('submissions.csv') 
grades = duckdb.read_csv('grades.csv') 

# Param
CONSECUTIVE_DAYS = 15

# Join all tables and add start_date column
data = duckdb.sql(f"""
    WITH joined AS
    (
        SELECT A.submission_id
            , B.student_id
            , C.test_id
            , A.submission_time
            , CAST(A.submission_time AS date) AS submission_date
            , D.grade 
        FROM submissions AS A
        INNER JOIN students AS B ON A.student_id = B.student_id
        INNER JOIN tests AS C ON A.test_id = C.test_id
        INNER JOIN grades AS D ON A.submission_id = D.submission_id
    )
    SELECT A.*
        , B.last_submission_date - INTERVAL 1 DAY * {CONSECUTIVE_DAYS-1} AS submission_start
    FROM joined AS A
    CROSS JOIN (
        SELECT MAX(submission_date) AS last_submission_date FROM joined
    ) AS B
""")

# Validate
if len(data.fetchall()) == 0:
    raise Exception('At least one CSV file is empty. Processing interrupted. Please provide non-empty CSV files.')

# Query 1
#   The number of unique students who submitted at least 1 test each day, 
#   for the last 15 days of data available.
query1 = duckdb.sql(f"""
    SELECT student_id
    FROM (
        SELECT DISTINCT student_id, submission_date
        FROM data
        WHERE submission_date >= submission_start
    )
    GROUP BY student_id
    HAVING COUNT(*) = {CONSECUTIVE_DAYS}
""")
print(f'\nResult #1: {len(query1.fetchall())}')

# Query 2
#   The number of unique students who submitted at least 1 valid test each day, 
#   for the last 15 days of data available.
query2 = duckdb.sql(f"""
    SELECT student_id
    FROM (
        SELECT DISTINCT student_id, submission_date
        FROM data
        WHERE submission_date >= submission_start
            AND grade > 0
    )
    GROUP BY student_id
    HAVING COUNT(*) = {CONSECUTIVE_DAYS}
""")
print(f'\nResult #2: {len(query2.fetchall())}')

# Query 3
#   The student with the most submissions for each day of the last 15 days of data available. 
#   If there are two or more students with the same results, print the student with the lowest ID.
# Query 3
#   The student with the most submissions for each day of the last 15 days of data available. 
#   If there are two or more students with the same results, print the student with the lowest ID.
query3 = duckdb.sql(f"""
    WITH source AS
    (
        SELECT student_id, submission_date, COUNT(*) AS submission_count
        FROM data
        WHERE submission_date >= submission_start
        GROUP BY student_id, submission_date
    )
    , sorted AS
    (
        SELECT *
            , ROW_NUMBER() OVER (
                PARTITION BY submission_date
                ORDER BY submission_count DESC, student_id
              ) AS sort
        FROM source
    )
    SELECT submission_date, student_id, submission_count AS count
    FROM sorted
    WHERE sort = 1
    ORDER BY submission_date
    
""")
print(f'\nResult #3:')
print('------------------------------------')
print('submission_date | student_id | count')
print('------------------------------------')
for row in query3.fetchall():
    print("{} | {} | {}".format(row[0].strftime('%Y-%m-%d').ljust(15), 
                                str(row[1]).rjust(10),
                                str(row[2]).rjust(5)))
print('------------------------------------')

# Query 4
#   The average grade for each test. If a student has submitted just once, 
#   consider that grade regardless of its value. If the student submitted 
#   the same test multiple times, give preference to the last valid grade.
#   
#   Processing steps:
#     1. Calculate the total grade per (student_id, test_id) -> needed in the next step.
#     2. Split data into 2 subsets:
#        - invalid_submissions: all submissions per (student_id, test_id) are invalid (grade=0) 
#          -> take distinct (student_id, test_id) rows with grade=0
#        - last_valid_submission: at least one submission is valid
#          -> take last valid submission per (student_id, test_id)
#     3. Union both subsets and compute the grade average per test_id.
query4 = duckdb.sql(f"""
    WITH source AS
    (
        SELECT student_id, test_id, grade, submission_time
            , SUM(grade) OVER (PARTITION BY student_id, test_id) AS total_grade
        FROM data
    )
    , invalid_submissions AS
    (
        SELECT DISTINCT student_id, test_id, grade
        FROM source
        WHERE total_grade = 0 
    )
    , last_valid_submissions AS
    (
        SELECT DISTINCT student_id, test_id, grade
            , ROW_NUMBER() OVER (
                PARTITION BY student_id, test_id
                ORDER BY submission_time DESC
              ) AS sort
        FROM source
        WHERE grade > 0 
    )
    , union_submissions AS
    (
        SELECT test_id, grade
        FROM last_valid_submissions
        WHERE sort = 1 
        UNION ALL
        SELECT test_id, 0
        FROM invalid_submissions
    )
    SELECT test_id
        , MEAN(grade) AS avg_grade
    FROM union_submissions
    GROUP BY test_id
    ORDER BY test_id
""")
print(f'\nResult #4:')
print('-------------------')
print('test_id | avg_grade')
print('-------------------')
for row in query4.fetchall():
    print("{} | {:.2f}".format(str(row[0]).rjust(7),
                               round(row[1], 2)))
print('-------------------')