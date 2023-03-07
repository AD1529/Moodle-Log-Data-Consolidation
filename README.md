# Moodle Log Data Consolidation
This repository contains the template for consolidating Moodle log data. It is based on the integration of logs extracted directly from your Moodle site and data extracted from the connected database.

## Quick start
### Collect your data
You first need to collect logs and database data:

- platform logs from [Moodle log generation interface](https://your_moodle_site/report/log/index.php?id=0)
(replace *your_moodle_site* with the address of your site). Please be aware that the *Manager* role is a minimum requirement to access the logs.
- database data
- course shortnames
- user roles

Export all files into *CSV* format.

### Queries
To access the database, you can install the [Configurable reports](https://moodle.org/plugins/block_configurable_reports) plugin. 
The following queries can be used to retrieve data from Moodle database.
#### Database data
```bash
SELECT id, userid, courseid, relateduserid, timecreated
FROM mdl_logstore_standard_log
```
#### Course shortname
```bash
SELECT id, shortname
FROM mdl_course
```
#### User roles
Query for student (role = 5), teacher (role = 3), and non-editing teacher (role = 4):
```bash
SELECT cx.instanceid as courseid, u.id as userid
FROM mdl_course c LEFT OUTER JOIN mdl_context cx ON c.id = cx.instanceid
LEFT OUTER JOIN mdl_role_assignments ra ON cx.id = ra.contextid AND ra.roleid = '???' AND cx.instanceid <> 1
LEFT OUTER JOIN mdl_user u ON ra.userid = u.id Where cx.contextlevel = '50'
```
Query for manager (role = 1) and course creator (role = 2):
```bash
SELECT distinct userid
FROM mdl_role_assignments
WHERE roleid = '???'
```

A complete list of roles is available in [Site administration > Users > Permissions > Define roles](https://your_moodle_site/admin/roles/manage.php) (replace *your_moodle_site* with the address of your site). Please be aware that the *Manager* role is a minimum requirement to access the roles.
To add new roles, you can integrate the function *add_role* in `src/algorithms/integrating.py`.

### Access your data
Put all files in the `src/datasets` folder. 
Replace paths in `src/paths.py`. 

### Get your consolidated data
Run the `main.py`.

According to your needs, you can modify *get_consolidated_data* function call.
If you want to get more fields, modify the variable *columns*.


## Get consolidated course data
Once the data has been consolidated, you can extract data from specific courses.

### Get specific data
You have first to create the object *Records* to use its methods. 
Then, to extract specific data, you can specify the following parameters: 'year', 'course_area', 'role', 'username'. 
Note that you may choose more than one entry, and that each entry must be provided as a list.
The entire dataset is returned if you make no selections.

You can also specify the 'dates_path'  containing the course dates to remove values that don't fall within the start and 
end dates.
You can get start and end dates by querying the database:
```bash
SELECT shortname, startdate, enddate 
FROM mdl_course
where id <> 1
```

### Clean the dataset
You can either clean the entire dataset or each course individually by modifying the function *clean_records* in
`src/algorithms/cleaning.py` according to your specific needs.

```bash
records = cl.clean_records(records)
or
course_A = cl.clean_records(course_A)
```
### Example

```bash
from src.classes.records import Records
import src.algorithms.cleaning as cl
import src.algorithms.extracting as ex
import pandas as pd
from src.paths import course_dates_path

# ------------
# GET DATA
# ------------
# get the consolidated dataframe
df_path = '../datasets/df_consolidated.csv'
df = pd.read_csv(df_path)

# create a Records object to use its methods
records = Records(df)

# ----------------------
# GET COURSES TO ANALYSE
# ----------------------
# select specific attributes to get the desired values
course_A = ex.extract_records(records, course_area=['Course A'], role=['Student'], datespath=course_dates_path)
course_B = ex.extract_records(records, year=[2021], username=['Student 01'])

# -----------------
# CLEAN THE DATASET
# -----------------
# you can either clean the entire dataset or each course individually
# records = cl.clean_records(records)
course_A = cl.clean_records(course_A)
```

## License

This project is licensed under the terms of the GNU General Public License v3.0.

If you use the library in an academic setting, please cite the following papers:

> Rotelli, Daniela, and Anna Monreale. "Time-on-task estimation by data-driven outlier detection based on learning activities", LAK22: 12th International Learning Analytics and Knowledge Conference, March 2022, Pages 336–346, https://doi.org/10.1145/3506860.3506913

```tex
@inproceedings{rotelli2022time,
  title={Time-on-task estimation by data-driven outlier detection based on learning activities},
  author={Rotelli, Daniela and Monreale, Anna},
  booktitle={LAK22: 12th International Learning Analytics and Knowledge Conference},
  pages={336--346},
  year={2022}
}
```

## Acknowledgements
This work has been partially supported by EU – Horizon 2020 Program under the scheme “INFRAIA-01-2018-2019 – Integrating 
Activities for Advanced Communities”, Grant Agreement n.871042, “SoBigData++: European Integrated Infrastructure for 
Social Mining and Big Data Analytics” (http://www.sobigdata.eu), the scheme "HORIZON-INFRA-2021-DEV-02 - Developing and 
consolidating the European research infrastructures landscape, maintaining global leadership (2021)", Grant Agreement 
n.101079043, “SoBigData RI PPP: SoBigData RI Preparatory Phase Project”, by NextGenerationEU - National Recovery and 
Resilience Plan (Piano Nazionale di Ripresa e Resilienza, PNRR) - Project: “SoBigData.it - Strengthening the Italian RI 
for Social Mining and Big Data Analytics” - Prot. IR0000013 - Avviso n. 3264 del 28/12/2021, and by PNRR - M4C2 - 
Investimento 1.3, Partenariato Esteso PE00000013 - ``FAIR - Future Artificial Intelligence Research" - Spoke 1 
"Human-centered AI", funded by the European Commission under the NextGeneration EU programme

## Contact(s)
[Daniela Rotelli](mailto:daniela.rotelli@phd.unipi.it) - Department of Computer Science - University of Pisa
