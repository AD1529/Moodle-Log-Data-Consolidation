# Moodle Log Data Consolidation
This repository contains the template for consolidating Moodle log data. It is based on the integration of logs extracted directly from the generation interface of your Moodle site and data extracted from the connected database.

## Table of contents
* [Collect your data](#collect-your-data)
* * [Queries](#queries)
* * [Data structure](#data-structure)
* [Access your data](#access-your-data)
* [Get your consolidated data](#get-your-consolidated-data)
* * [Example](#example)
* [License](#license)
* [Acknowledgments](#acknowledgements)
* [Contacts](#contact--s-)


## Quick start
This section contains a description of the data structure expected by the algorithms, as well as instructions on how 
collecting your data, accessing and consolidating them.
### Collect your data
You first need to collect logs and database data:

- platform logs from your [Moodle log generation interface](https://your_moodle_site/report/log/index.php?id=0). Please be aware that the *Manager* role is a minimum requirement to access the logs.
- database data
- course shortnames
- user roles
- deleted users

#### Queries
To access the database, you can install the [Configurable reports](https://moodle.org/plugins/block_configurable_reports) plugin. 
The following queries can be used to retrieve data from Moodle database.
##### Database data
```SQL
SELECT id, userid, courseid, relateduserid, timecreated
FROM mdl_logstore_standard_log
```
##### Course shortname
```SQL
SELECT id, shortname
FROM mdl_course
```
##### User roles
Query for student (role = 5), teacher (role = 3), and non-editing teacher (role = 4):
```SQL
SELECT cx.instanceid as courseid, u.id as userid
FROM mdl_course c LEFT OUTER JOIN mdl_context cx ON c.id = cx.instanceid
LEFT OUTER JOIN mdl_role_assignments ra ON cx.id = ra.contextid AND ra.roleid = '???' AND cx.instanceid <> 1
LEFT OUTER JOIN mdl_user u ON ra.userid = u.id WHERE cx.contextlevel = '50'
```
Query for manager (role = 1) and course creator (role = 2):
```SQL
SELECT distinct userid
FROM mdl_role_assignments
WHERE roleid = '???'
```

Query for admin role:
```SQL
SELECT value
FROM mdl_config
WHERE name = 'siteadmins'
```

A complete list of roles is available in [Site administration > Users > Permissions > Define roles](https://your_moodle_site/admin/roles/manage.php). Please be aware that the *Manager* role is a minimum requirement to access the roles.
To add new roles, you can integrate the function *add_role* in `src/algorithms/integrating.py`.

##### Deleted users
You may choose to purge records of deleted users. Get the list of deleted users' id from the database.

Query for deleted users:
```SQL
SELECT id
FROM mdl_user
WHERE deleted = 1
```

#### Data structure
The collected log file should contain at least the following columns:
- _Time_ - the sequence timestamp
- _Username_ - the username of the user performing the action
- _Affected_user_ - the eventual recipient of the action (for instance, the user who receives a message)
- _Event_context_ - the context within the platform [^1]
- _Component_ - the module type (e.g., Wiki, Page, File, Url, Quiz)
- _Event_name_ - the type of action performed on the module (such as viewed, deleted, updated, created, and submitted)
- _Description_ - the description of the event
- _Origin_ - the selected (CLI, web, restore, and web service)
- _IP_address_ - the ip location
- _ID_ - the sequence id
- _userid_ - the user id of the user performing the action
- _courseid_ - the course id where the action is performed
- _relateduserid_ - the user id of the affected user

Export all files into *CSV* format.

[^1] : MoodleDocs - [Context](https://docs.moodle.org/39/en/Context)

### Access your data
You have the option of exporting multiple files, one for each user/course, or a single file for one or multiple users/courses.
Single *CSV* file should be placed in the `src/datasets/` folder. Multiple files should be placed in 
the `src/datasets/directory` folder.

Replace path names in the `src/paths.py` file. The `src/datasets` folder contains examples of the expected files. 

### Get your consolidated data
Make sure that you have all the necessary libraries, modules, and packages installed on your machine.
```bash
pip install -r requirements.txt
```
Run `main.py`.

Please be aware, that if you use a unique directory with multiple files, you have to modify the platforms logs with
the directory in the `get_consolidated_data` function call: 
    
`df = get_consolidated_data(directory=directory_path, course_shortnames=example_course_shortnames_path)`

According to your needs, you can also modify the `get_consolidated_data` function.

After data consolidation, the collected log file will contain the following columns:
- _ID_ - the sequence id
- _Time_ - the date and time of the action
- _Year_ - the year of the course. Remove this field if you are only analysing data from a specific year. 
- _Course_Area_ - the area of the platform or the course name
- _Unix_Time_ - the Unix timestamp
- _Username_ - the username of the user performing the action
- _Component_ - the module type (e.g., Wiki, Page, File, Url, Quiz)
- _Event_name_ - the type of action performed on the module (such as viewed, deleted, updated, created, and submitted)
- 'Role' - student, teacher, admin, course creator, guest, non-editing teacher
- _userid_ - the user id of the user performing the action
- _courseid_ - the course id where the action is performed
- 'Status' - status indicating whether the event was executed on a deleted activity or module

### Clean the dataset
You can clean the dataset by modifying functions in `src/algorithms/cleaning.py` file according to your needs.

## Get course data
Once the data has been consolidated, you can extract specific data.

### Get specific data
You have first to create the object *Records* to use its methods. 
Then, to extract specific data, you can specify the following parameters: 'year', 'course_area', 'role', 'username'. 
Note that you may choose more than one entry, and that each entry must be provided as a list.
The entire dataset is returned if you make no selections.

You can also specify the 'dates_path'  containing the course dates to remove values that don't fall within the start and 
end dates.
You can get start and end dates by querying the database:
```SQL
SELECT id, shortname, startdate, enddate 
FROM mdl_course
where id <> 1
```

### Example

```bash
from src.classes.records import Records
import src.algorithms.extracting as ex
import pandas as pd
from src.paths import COURSE_DATES_PATH

# ------------
# GET DATA
# ------------
# get the consolidated dataframe
df_path = 'src/datasets/df_consolidated.csv'
df = pd.read_csv(df_path)

# create a Records object to use its methods
records = Records(df)

# ----------------------
# GET COURSES TO ANALYSE
# ----------------------
# select specific attributes to get the desired values
course_A = ex.extract_records(records, course_area=['Course A'], role=['Student'], filepath=COURSE_DATES_PATH)
course_B = ex.extract_records(records, username=['Student 01'])
```
## License

This project is licensed under the terms of the GNU General Public License v3.0.

If you use the template in an academic setting, please cite the following papers:

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
> Rotelli, Daniela, and Anna Monreale. "Processing and Understanding Moodle Log Data and their Temporal Dimension", Journal of Learning Analytics, 2023

```tex
@article{rotelli2023processing,
  title={Processing and Understanding Moodle Log Data and their Temporal Dimension},
  author={Rotelli, Daniela and Monreale, Anna},
  booktitle={Journal of Learning Analytics},
  year={2023}
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
