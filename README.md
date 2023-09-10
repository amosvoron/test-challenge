# Test Challenge Solution

### /data/solution

You will find 4 solutions:

* pandas
* pyspark
* sql
* python

Each solution uses the technology according to its name.

**REMARK on ambiguity of the 4th query**

*The average grade for each test. If a student has submitted just once, consider that grade regardless of its value. If the student submitted the same test multiple times, give preference to the last valid grade.*

Overall, there are 3 possible types of submissions per (test, student) - TS:

1. A single submission per TS.
2. Multiple submissions per TS where at least one submission is valid (grade > 0).
3. Multiple submissions per TS where all submissions are invalid (grade = 0).

The instructions cover the possibility 1 and 2, but not 3. In our solution, the case 3 is treated the same way as the case 1, therefore we take any instance TS irrespective of whether it is valid or not. Otherwise, we would have that the data point of a student with a single invalid submission T is accepted while the data point of a student with 3 invalid submissions T is rejected which simply doesn't seem logical.

All solutions produce the same test results.

### /sql/postgres

* query.sql

### /ts

* Node.js application to track the asteroids from the NASA list
* All the required features included:
* > - Display a list of asteroids
  > - Search by a range of dates
  > - See the detail of the asteroids by clicking on one of the items
  > - Sort the asteroids by name
  >
* The exception handling through middleware that redirects errors to the separate error view
* Note that in case of too many requests to the NASA server you might get the "Too many requests" error 429. In that case the data is fetched from the local JSON file (a snapshot of the demo data from NASA on 2015-09-07).

**Margin to improve**

When the date interval is entered in the search engine, the search request that provides the new data does not handle correctly the search parameters (date interval) which are lost when the page is reloaded and reset to their default values.
