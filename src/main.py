import src.algorithms.integrating as it
import src.algorithms.cleaning as cl
import src.algorithms.transforming as tr
from pandas import DataFrame


def get_consolidated_data(platform_logs: str or DataFrame,
                          database_data: str,
                          course_shortnames: str,
                          student_role: str,
                          teacher_role: str = "",
                          non_editing_teacher_role: str = "",
                          course_creator_role: str = "",
                          manager_role: str = "",
                          directory: str = "") -> DataFrame:
    """

    Args:
        platform_logs: str,
            The path to platform logs.
        database_data: str,
            The path to database data.
        course_shortnames: str,
            The path to course shortnames.
        student_role: str,
            The path to students data.
        teacher_role: str, optional
            The path to teachers data.
        non_editing_teacher_role: str, optional
            The path to non-editing teachers data.
        course_creator_role: str, optional
            The path to course creator data.
        manager_role: str, optional
            The path to manager data.
        directory:  str, optional
            The path to the directory containing logs extracted user by user.

    Returns:
        The consolidated log data.

    """

    # --------------------
    # DATA INTEGRATION
    # --------------------
    # collect users data if extracted user by user
    if directory != '':
        platform_logs = it.collect_user_logs(directory)
    # join the platform and the database data
    joined_logs = it.get_joined_logs(platform_logs, database_data)
    # add course shortname
    joined_logs = it.add_course_shortname(joined_logs, course_shortnames)
    # add year to platform logs
    joined_logs = it.add_year(joined_logs)
    # add roles : teacher and student
    joined_logs = it.add_role(joined_logs, student_role, teacher_role,
                              non_editing_teacher_role, course_creator_role, manager_role)
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
    joined_logs = tr.set_data_types(joined_logs)
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
    # columns of the dataframe
    # Time, Username, Affected_user, Event_context, Component, Event_name, Description, Origin, IP_address, ID, user_id,
    # course_id, related_user_id, Unix_Time, Course_Area, Year, Role

    # select and reorder columns
    columns = ['ID', 'Time', 'Year', 'Course_Area', 'Unix_Time', 'Username', 'Component', 'Event_name', 'Role',
               'user_id']
    # drop unused columns
    joined_logs = joined_logs[columns].copy()

    return joined_logs


if __name__ == '__main__':

    from src.paths import *

    # get the consolidated dataframe
    df = get_consolidated_data(platform_logs=platform_logs_path,
                               database_data=database_data_path,
                               course_shortnames=course_shortnames_path,
                               student_role=student_role_path,
                               teacher_role=teacher_role_path,
                               manager_role=manager_role_path)

    # clean useless data from the entire dataset
    df = cl.clean_dataset_records(df)

    # you can save the dataset for further analysis
    df.to_csv('../datasets/df_consolidated.csv')
