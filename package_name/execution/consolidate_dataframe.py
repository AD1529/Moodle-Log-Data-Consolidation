import package_name.algorithms.integrating as it
import package_name.algorithms.cleaning as cl
import package_name.algorithms.transforming as tr
from pandas import DataFrame


def get_consolidated_logs(platform_logs: str or DataFrame,
                          database_logs: str,
                          course_shortname_year_path: str,
                          course_students_path: str,
                          course_teachers_path: str = "",
                          course_non_editing_teachers_path: str = "",
                          managers_path: str = "",
                          directory_path: str = "") -> DataFrame:

    # --------------------
    # DATA INTEGRATION
    # --------------------
    # collect users data
    # ###platform_logs = collect_user_logs(directory_path)###
    # join the platform and the database logs
    joined_logs = it.get_joined_logs(platform_logs, database_logs)
    # add course shortname and year of the courses under analysis
    joined_logs = it.add_course_shortname_and_year(joined_logs, course_shortname_year_path)
    # add year to platform logs
    joined_logs = it.add_year(joined_logs)
    # add roles : teacher and student
    joined_logs = it.add_role(joined_logs, course_students_path, course_teachers_path,
                              course_non_editing_teachers_path, managers_path)
    # add the area to platform logs
    joined_logs = it.course_area_categorisation(joined_logs)
    # redefine components
    joined_logs = it.component_redefinition(joined_logs)

    # --------------------
    # DATA TRANSFORMATION
    # --------------------
    # rename the dataframe columns
    joined_logs = tr.rename_columns(joined_logs)
    # convert data types for further analysis
    joined_logs = tr.convert_data_types(joined_logs)
    # convert the timestamps in a human-readable format
    joined_logs = tr.make_timestamp_readable(joined_logs)

    # --------------------
    # DATA CLEANING
    # --------------------
    # remove admin, cron, and guest records
    joined_logs = cl.remove_admin_cron_guest_records(joined_logs)
    # remove records with no course
    joined_logs = cl.remove_records_left(joined_logs)

    # --------------------
    # DATA SELECTION
    # --------------------
    # select and reorder columns
    columns = ['ID', 'Time', 'Year', 'Course_Area', 'Unix_Time', 'Username',
               'Component', 'Event_name', 'Role']
    # drop unused columns
    joined_logs = joined_logs[columns].copy()

    return joined_logs
