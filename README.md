# Moodle Log Data Consolidation
This repository contains the template for consolidating Moodle log data. It is based on the integration of logs extracted directly from your Moodle site and data extracted from the connected database.

## Quick start
### Collect your data
You first need to collect logs and database data:

- platform logs from [Moodle log generation interface](https://your_moodle_site/report/log/index.php?id=0)
(replace your_moodle_site with the address of your site). Please be aware that the *Manager* role is a minimum requirement to access the logs.
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

A complete list of roles is available from. Site [administration > Users > Permissions > Define roles](https://your_moodle_site/admin/roles/manage.php) (replace your_moodle_site with the address of your site). Please be aware that the *Manager* role is a minimum requirement to access the logs.
You have then to modify or integrate the function *add_role* in `package_name/algorithms/integrating.py`

### Access your data
Put all files in the `package_name/datasets` folder. 
Replace paths in `package_name/execution/paths.py`. 

### Get your consolidated data
Modify the parameters of the function *get_consolidated_data* in `package_name/execution/data_consolidation.py` adding
all paths according to your needs and run.
Go to `package_name/execution/consolidate_dataframe.py` to change the final resultant fields. 

## Get consolidated course data
Once the data has been consolidated, you can extract data from specific courses.

### Get specific data
You have first to create the object *Records* to use its methods. 
Then, to extract specific data, you can specify the following parameters: 'year', 'course_area', 'role', 'username'. 
Note that you may choose more than one entry, and that each entry must be provided as a list.
The entire dataset is returned if you make no selections.

You can also specify the 'dates_path' containing the course dates to remove values that don't fall within the start and 
end dates.
You can get start and end dates by querying the database:
```bash
SELECT shortname, startdate, enddate 
FROM mdl_course
```
An example of specific course data extraction is provided in `package_name/execution/extract_course_data.py`. 

### Clean the dataset
You can either clean the entire dataset or each course individually by modifying the function *clean_records* in
`package_name/algorithms/cleaning.py` according to your specific needs.

```bash
records = cl.clean_records(records)
or
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
