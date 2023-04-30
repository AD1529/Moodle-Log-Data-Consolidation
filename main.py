import src.algorithms.integrating as it
import src.algorithms.cleaning as cl
import src.algorithms.transforming as tr
from pandas import DataFrame


def get_consolidated_data(database_data: str,
                          course_shortnames: str,
                          student_role: str,
                          platform_logs: str = "",
                          teacher_role: str = "",
                          non_editing_teacher_role: str = "",
                          course_creator_role: str = "",
                          manager_role: str = "",
                          admin_role: str = "",
                          deleted_users: str = "",
                          directory: str = "") -> DataFrame:
    """
    Get consolidated dataframe.

    Args:
        platform_logs: The path to platform logs.
        database_data: The path to database data.
        course_shortnames: The path to course shortnames.
        student_role: The path to students data.
        teacher_role: The path to teachers data; optional.
        non_editing_teacher_role: The path to non-editing teachers data; optional.
        course_creator_role: The path to course creator data; optional.
        manager_role: The path to manager data; optional.
        admin_role: The path to admin data; optional.
        deleted_users: The path to deleted users data; optional.
        directory: The path to the directory containing logs extracted user by user.

    Returns:
        The consolidated dataframe.
    """

    # --------------------
    # DATA INTEGRATION
    # --------------------
    # collect users data if extracted user by user
    if directory != '':
        platform_logs = it.collect_user_logs(directory)
    # join the platform and the database data
    log_data = it.get_joined_logs(platform_logs, database_data)
    # add course shortnames
    log_data = it.add_course_shortname(log_data, course_shortnames)
    # add year to platform logs
    log_data = it.add_year(log_data)
    # add the area to platform logs
    log_data = it.redefine_course_area(log_data)
    # redefine components
    log_data = it.redefine_component(log_data)
    # add roles
    log_data = it.add_role(log_data, student_role, teacher_role, non_editing_teacher_role,
                           course_creator_role, manager_role, admin_role)
    # identify actions on deleted modules
    log_data = it.identify_deleted_modules(log_data)

    # --------------------
    # DATA TRANSFORMATION
    # --------------------
    # convert the timestamps to human-readable format
    log_data = tr.make_timestamp_readable(log_data)

    # --------------------
    # DATA CLEANING
    # --------------------
    # remove deleted users if any
    log_data = cl.remove_deleted_users(log_data, deleted_users)
    # remove automatic events
    log_data = cl.remove_automatic_events(log_data)

    # --------------------
    # DATA SELECTION
    # --------------------
    # select and reorder columns
    COLUMNS = ['ID', 'Time', 'Year', 'Course_Area', 'Unix_Time', 'Username', 'Component', 'Event_name', 'Role',
               'userid', 'courseid', 'Status']

    # drop unused columns
    log_data = log_data[COLUMNS].copy()

    return log_data


if __name__ == '__main__':

    from src.paths import *

    # get the consolidated dataframe
    df = get_consolidated_data(platform_logs=PLATFORM_LOGS_PATH,
                               database_data=DATABASE_DATA_PATH,
                               course_shortnames=COURSE_SHORTNAMES_PATH,
                               student_role=STUDENT_ROLE_PATH,
                               teacher_role=TEACHER_ROLE_PATH,
                               non_editing_teacher_role=NON_EDITING_TEACHER_ROLE_PATH,
                               manager_role=MANAGER_ROLE_PATH,
                               admin_role=ADMIN_ROLE_PATH,
                               deleted_users=DELETED_USERS_PATH)

    # remove useless data from the entire dataset
    # df = cl.clean_dataset_records(df)

    # you can save the dataset for further analysis
    df.to_csv('src/datasets/consolidated_df.csv')
