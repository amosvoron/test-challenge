from pyspark.sql import SparkSession, Row
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import udf as udf
from pyspark.sql.functions import col as col
from datetime import datetime, timedelta

spark = SparkSession.builder.appName('TestChallenge#1App').getOrCreate()

# Load all CSVs
students = spark.read.csv('students.csv', header=True, inferSchema=True)
tests = spark.read.csv('tests.csv', header=True, inferSchema=True)
submissions = spark.read.csv('submissions.csv', header=True, inferSchema=True)
grades = spark.read.csv('grades.csv', header=True, inferSchema=True)

# Param
CONSECUTIVE_DAYS = 15

# Join all dataframes
df = students \
    .join(submissions, on='student_id') \
    .join(tests, on='test_id') \
    .join(grades, on='submission_id')

# Cast submission_time into date 
df = df.withColumn('submission_date', 
                   F.to_date(F.substring('submission_time', 1, 10),'yyyy-MM-dd'))

#  Validate
if df.count() == 0:
    raise Exception('At least one CSV file is empty. Processing interrupted. Please provide non-empty CSV files.')

# Calculate min/max dates
max_date = df.agg(F.max('submission_date').alias('max_date')).collect()[0].max_date
min_date = max_date - timedelta(days=CONSECUTIVE_DAYS-1)

# Add submission_start column
df = df.withColumn('submission_start', F.lit(min_date))

# ----------
#  Query 1
# ----------

result1 = df \
    .where(col('submission_date') >= col('submission_start')) \
    .select('student_id', 'submission_date') \
    .distinct() \
    .groupby('student_id') \
    .count() \
    .where(col('count') == CONSECUTIVE_DAYS) \
    .count()
print(f'\nResult #1: {result1}')

# ----------
#  Query 2
# ----------

result2 = df \
    .where(col('submission_date') >= col('submission_start')) \
    .where(col('grade') > 0) \
    .select('student_id', 'submission_date') \
    .distinct() \
    .groupby('student_id') \
    .count() \
    .where(col('count') == CONSECUTIVE_DAYS) \
    .count()
print(f'\nResult #2: {result2}')

# ----------
#  Query 3
# ----------

print('\nResult #3:')
df \
    .where(col('submission_date') >= col('submission_start')) \
    .groupby('student_id', 'submission_date') \
    .count() \
    .withColumn('sort', F.row_number().over(Window.partitionBy('submission_date')                                                                                         .orderBy(F.desc('count'), 'student_id'))) \
    .where(col('sort') == 1) \
    .select('submission_date', 'student_id', 'count') \
    .sort('submission_date') \
    .show(50)

# ----------
#  Query 4
# ----------

# Add count and sum(grade) per (student, test) group
df2 = df.select('student_id', 'test_id', 'submission_time', 'grade') \
    .withColumn('count', F.count('*').over(Window.partitionBy('student_id', 'test_id'))) \
    .withColumn('grade_total', F.sum('grade').over(Window.partitionBy('student_id', 'test_id')))

# Extract single (student, test) submissions
single_df = df2.where(col('count') == 1).drop('count')

# Extract multiple (student, test) submissions where all submissions are invalid
multiple_invalid_df = df2 \
    .where(col('count') > 1).drop('count') \
    .where(col('grade_total') == 0) 

# Extract multiple (student, test) submissions with at least one valid submission
# and filter out the invalid submissions
multiple_valid_df = df2 \
    .where(col('count') > 1).drop('count') \
    .where(col('grade_total') > 0) \
    .where(col('grade') > 0)

# Union all 3 sub-datasets
df3 = single_df \
    .unionByName(multiple_invalid_df) \
    .unionByName(multiple_valid_df)

# Select last (student, test) row and compute the grade average
result4 = df3 \
    .withColumn('sort', F.row_number().over(Window.partitionBy('student_id', 'test_id')                                                                                   .orderBy(F.desc('submission_time')))) \
    .where(col('sort') == 1) \
    .groupby('test_id') \
    .agg(F.mean('grade').alias('avg_grade'))

print('Result #4:')
result4.sort('test_id').show()

spark.stop()