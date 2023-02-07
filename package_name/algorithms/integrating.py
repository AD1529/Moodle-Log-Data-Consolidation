import pandas as pd
from pandas import DataFrame
import glob


def get_dataframe(file_path: str, columns: [] = None) -> DataFrame:

    df = pd.read_csv(file_path, sep=',')

    # add column names if missing
    try:
        value_type = int(df.columns[0])
        if isinstance(value_type, int):
            df = pd.read_csv(file_path, sep=',', header=None)
            df.columns = columns
    except ValueError:
        pass

    return df


def collect_user_logs(directory_path: str) -> DataFrame:
    """
    e' possibile collezione i dati degli utenti in caso di troppi dati da moodle
    Args:
        directory_path:

    Returns:

    """

    global_table = pd.DataFrame()

    for file_path in glob.glob(directory_path + '*.csv'):
        # get the user logs
        user_logs = get_dataframe(file_path)
        # concatenate the user logs to the global table
        global_table = pd.concat([global_table, user_logs], axis=0)

    # reset the index
    global_table.reset_index(drop=True)

    return global_table


def get_joined_logs(platform: str or DataFrame,
                    database: str) -> DataFrame:

    """
    first collect the site logs from Moodle platform and the timestamps directly from the database.
    Timestamps are reversed since the first log identifies the first action recorded, whereas the first log extracted
    from Moodle interface represents the last action.
    Some values may not be merged because the lengths of the two files may not be equal, thus they must be aligned.

    se i dati sono collezionati per utente anche quando si estraggono i dati dal database, andranno selezionati solo
    gli utenti che si vogliono analizzare
    Args:

        platform: str,
            platform_logs columns: ['Time', 'User full name', 'Affected user', 'Event context', 'Component',
                                    'Event name', 'Description', 'Origin', 'IP address']
        database: str

            database_logs columns: ['id', 'userid', 'courseid', 'relateduserid', 'timecreated']

            select 'id', 'userid', 'courseid', 'relateduserid', 'timecreated'
            from prefix_logstore_standard_log
            # where userid = 34 or userid = 45
    Returns:

    """

    if isinstance(platform, str):
        # import the complete set of logs extracted from Moodle log extraction interface
        platform_logs = get_dataframe(platform)
    else:
        # if the parameter is a DataFrame
        platform_logs = platform

    # import the logs extracted from database with ['userid', 'courseid', 'relateduserid', 'timecreated']
    database_logs = get_dataframe(database, columns=['id', 'userid', 'courseid', 'relateduserid', 'timecreated'])

    # to_remove = (database_logs.loc[database_logs.userid == -1]).index
    # df_remove.drop(to_remove, axis=0, inplace=True)

    # reverse platform logs
    platform_logs = platform_logs[::-1].copy()
    platform_logs = platform_logs.reset_index(drop=True)

    # sort database logs
    database_logs = database_logs.sort_values(by=['timecreated', 'id'])
    database_logs = database_logs.reset_index(drop=True)

    # align the number of records for both dataframes
    if len(platform_logs) != len(database_logs):
        n = abs(len(database_logs) - len(platform_logs))
        # drop the exceeding rows
        if len(database_logs) > len(platform_logs):
            to_drop = list(database_logs.tail(n).index)
            database_logs = database_logs.drop(to_drop)
        else:
            to_drop = list(platform_logs.tail(n).index)
            platform_logs = platform_logs.drop(to_drop)

    # concatenate data
    joined_logs = pd.concat([platform_logs, database_logs], axis=1)

    return joined_logs


def add_course_shortname_and_year(df: DataFrame, file_path: str) -> DataFrame:

    df_course_names = get_dataframe(file_path)
    shortnames = df_course_names['shortname']

    for shortname in shortnames:
        courseid = df_course_names.loc[df_course_names['shortname'] == shortname]['id'].values[0]
        df.loc[df['courseid'] == courseid, 'Course'] = shortname.split('_')[0]
        df.loc[df['courseid'] == courseid, 'Year'] = shortname.split('_')[1]

    return df


def add_role(df: DataFrame,
             course_students_path: str,
             course_teachers_path: str = "",
             course_non_editing_teachers_path: str = "",
             managers_path: str = "") -> DataFrame:

    """Importante: quando aggiungo il ruolo, viene dato come student o teacher solo all'interno del corso
    l'utente fuori dal corso non è più studente.


    SELECT cx.instanceid as courseid, u.id as userid
    FROM prefix_course c LEFT OUTER JOIN prefix_context cx ON c.id = cx.instanceid
    LEFT OUTER JOIN prefix_role_assignments ra ON cx.id = ra.contextid AND ra.roleid = '3/4/5' AND cx.instanceid <> 1
    LEFT OUTER JOIN prefix_user u ON ra.userid = u.id Where cx.contextlevel = '50'

    3 = teachers, 4 = non-editing teachers, 5 = students

    select distinct userid
    from prefix_role_assignments
    where roleid = '1/2'

    1 = manager = 1, 2 = course creator


    """

    # students
    course_students = get_dataframe(course_students_path, columns=['courseid', 'userid'])
    for idx in range(len(course_students)):
        df.loc[(df['courseid'] == course_students.iloc[idx]['courseid']) &
               (df['userid'] == course_students.iloc[idx]['userid']), 'Role'] = 'Student'

    # teachers
    if course_teachers_path != "":
        course_teachers = get_dataframe(course_teachers_path, columns=['courseid', 'userid'])
        for idx in range(len(course_teachers)):
            df.loc[(df['courseid'] == course_teachers.iloc[idx]['courseid']) &
                   (df['userid'] == course_teachers.iloc[idx]['userid']), 'Role'] = 'Teacher'

    # non-editing teachers
    if course_non_editing_teachers_path != "":
        course_non_editing_teachers = get_dataframe(course_non_editing_teachers_path, columns=['courseid', 'userid'])
        for idx in range(len(course_non_editing_teachers)):
            df.loc[(df['courseid'] == course_non_editing_teachers.iloc[idx]['courseid']) &
                   (df['userid'] == course_non_editing_teachers.iloc[idx]['userid']), 'Role'] = 'Non-editing Teacher'

    # managers
    if managers_path != "":
        managers = get_dataframe(managers_path, columns=['roleid', 'userid'])
        for idx in range(len(managers)):
            df.loc[(df['userid'] == managers.iloc[idx]['userid']) &
                   (df['Role'].isnull()), 'Role'] = 'Manager'

    # admin
    df.loc[df['userid'] == 2, 'Role'] = 'Admin'

    # guest and users who access the course just to have a look then they unenroll
    df.loc[df['userid'] == 1, 'Role'] = 'Guest'
    df.loc[(df['Role'].isnull()) &
           (df['Course'].notnull()) &
           (df['User full name'] != '-'), 'Role'] = 'Guest'

    # authenticated user
    df.loc[(df['Role'].isnull()) &
           (df['User full name'] != '-'), 'Role'] = 'Authenticated user'

    return df


def add_year(df: DataFrame) -> DataFrame:

    df.loc[df['Year'].isnull(), 'Year'] = df.loc[df['Year'].isnull()]['Time'].\
        map(lambda x: int(x.split('/')[2].split(',')[0]) + 2000)

    return df


def course_area_categorisation(df: DataFrame) -> DataFrame:

    # authentication
    df.loc[df['Event name'] == 'User has logged in', 'Course'] = 'Authentication'
    df.loc[df['Event name'] == 'User login failed', 'Course'] = 'Authentication'
    df.loc[df['Event name'] == 'User logged out', 'Course'] = 'Authentication'

    # mobile
    df.loc[df['Event name'].str.contains('Web service'), 'Course'] = 'Mobile'

    # overall site
    df.loc[df['Event name'].str.contains('Dashboard'), 'Course'] = 'Overall Site'
    df.loc[df['Event context'].str.contains('(?i)Category'), 'Course'] = 'Overall Site'
    df.loc[df['Event name'].str.contains('(?i)Category'), 'Course'] = 'Overall Site'
    df.loc[df['Event name'].str.contains('Calendar'), 'Course'] = 'Overall Site'
    df.loc[df['Event name'] == 'Courses searched', 'Course'] = 'Overall Site'
    df.loc[df['Event context'] == 'Front page', 'Course'] = 'Overall Site'
    df.loc[df['Event context'] == 'Forum: Site announcements', 'Course'] = 'Overall Site'
    df.loc[df['Event name'] == 'Notification viewed', 'Course'] = 'Overall Site'
    df.loc[(df['Event name'] == 'Blog entries viewed') &
           (df['Affected user'] == df['User full name']), 'Course'] = 'Overall Site'
    df.loc[(df['Event name'] == 'User report viewed') &
           (df['Affected user'] == df['User full name']) & (df['Component'] == 'Forum'), 'Course'] = 'Overall Site'
    df.loc[df['Event context'] == 'Forum: Site announcements', 'Course'] = 'Overall Site'

    # profile
    df.loc[df['Event name'].str.contains('User password'), 'Course'] = 'Profile'
    df.loc[df['Event name'] == 'User updated', 'Course'] = 'Profile'
    df.loc[(df['Event name'] == 'User profile viewed') &
           (df['Affected user'] == df['User full name']), 'Course'] = 'Profile'
    df.loc[(df['Event name'] == 'Badge viewed') & (df['Event context'] == 'System'), 'Course'] = 'Profile'
    df.loc[(df['Event name'] == 'Tag added to an item') &
           (df['Event context'].str.contains('User:')), 'Course'] = 'Profile'
    df.loc[(df['Event name'] == 'Tag removed from an item') &
           (df['Event context'].str.contains('User:')), 'Course'] = 'Profile'
    df.loc[df['Event name'] == 'Tag created', 'Course'] = 'Profile'
    df.loc[df['Event name'] == 'Tag deleted', 'Course'] = 'Profile'
    df.loc[(df['Event name'] == 'Course user report viewed') &
           (df['Affected user'] == df['User full name']), 'Course'] = 'Profile'
    df.loc[(df['Event name'] == 'Notes viewed') &
           (df['Affected user'] == df['User full name']), 'Course'] = 'Profile'

    # social interaction
    df.loc[(df['Event name'].str.contains('(?i)message')) &
           (df['Component'] != 'Chat'), 'Course'] = 'Social interaction'
    df.loc[(df['Event name'] == 'Notification sent') &
           (df['Affected user'] != df['User full name']), 'Course'] = 'Social interaction'
    df.loc[(df['Event name'] == 'User profile viewed') &
           (df['Affected user'] != df['User full name']), 'Course'] = 'Social interaction'
    df.loc[(df['Event name'] == 'Blog entries viewed') &
           (df['Affected user'] != df['User full name']), 'Course'] = 'Social interaction'

    return df


def component_redefinition(df: DataFrame) -> DataFrame:

    # assignment
    df.loc[df['Component'].str.contains('(?i)submission'), 'Component'] = 'Assignment'

    # authentication
    df.loc[df['Event name'] == 'User has logged in', 'Component'] = 'Login'
    df.loc[df['Event name'] == 'User login failed', 'Component'] = 'Login'
    df.loc[df['Event name'] == 'User logged out', 'Component'] = 'Logout'

    # backup
    df.loc[df['Component'].str.contains('backup'), 'Component'] = 'Backup'

    # badge
    df.loc[df['Event name'].str.contains('Badge'), 'Component'] = 'Badge'

    # blog
    df.loc[df['Event name'].str.contains('Blog'), 'Component'] = 'Blog'

    # book
    df.loc[df['Component'] == 'Book printing', 'Component'] = 'Book'

    # calendar
    df.loc[df['Event name'].str.contains('Calendar'), 'Component'] = 'Calendar'

    # capability
    df.loc[df['Event name'].str.contains('Capability'), 'Component'] = 'Capability'

    # course activity completion updated
    ccu = list(df.loc[df['Event name'] == 'Course activity completion updated'].index)
    for idx in ccu:
        df.loc[idx, 'Component'] = df.loc[idx, 'Event context'].split(':')[0]

    # course home
    df.loc[df['Event name'].str.contains('Course section'), 'Component'] = 'Course home'
    df.loc[(df['Event context'].str.contains('Course')) &
           (df['Event name'] == 'Course viewed'), 'Component'] = 'Course home'

    # course module created
    cmc = list(df.loc[df['Event name'] == 'Course module created'].index)
    for idx in cmc:
        df.loc[idx, 'Component'] = df.loc[idx, 'Event context'].split(':')[0]

    # course module updated
    cmu = list(df.loc[df['Event name'] == 'Course module updated'].index)
    for idx in cmu:
        df.loc[idx, 'Component'] = df.loc[idx, 'Event context'].split(':')[0]

    # courses list
    df.loc[df['Event name'] == 'Category viewed', 'Component'] = 'Courses list'
    df.loc[df['Event name'] == 'Courses searched', 'Component'] = 'Courses list'

    # dashboard
    df.loc[df['Event name'].str.contains('Dashboard'), 'Component'] = 'Dashboard'

    # enrollment
    df.loc[df['Event name'] == 'User enrolled in course', 'Component'] = 'Enrollment'
    df.loc[df['Event name'] == 'User unenrolled from course', 'Component'] = 'Enrollment'

    # grade-book
    df.loc[df['Component'] == 'Single view', 'Component'] = 'Gradebook'
    df.loc[df['Component'] == 'Excel spreadsheet', 'Component'] = 'Gradebook'
    df.loc[df['Component'] == 'OpenDocument spreadsheet', 'Component'] = 'Gradebook'
    df.loc[df['Component'] == 'Grader report', 'Component'] = 'Gradebook'
    df.loc[df['Component'] == 'Outcomes report', 'Component'] = 'Gradebook'
    df.loc[df['Event name'] == 'User graded', 'Component'] = 'Gradebook'
    df.loc[df['Event name'] == 'Grade item updated', 'Component'] = 'Gradebook'
    df.loc[df['Event name'] == 'Grade deleted', 'Component'] = 'Gradebook'
    df.loc[df['Event name'] == 'Grade item created', 'Component'] = 'Gradebook'
    df.loc[df['Event name'] == 'Scale created', 'Component'] = 'Gradebook'
    df.loc[df['Event name'] == 'Scale deleted', 'Component'] = 'Gradebook'

    # grades
    df.loc[df['Event name'] == 'Grade overview report viewed', 'Component'] = 'Grades'
    df.loc[df['Event name'] == 'Course user report viewed', 'Component'] = 'Grades'
    df.loc[df['Component'] == 'User report', 'Component'] = 'Grades'

    # groups
    df.loc[(df['Event name'].str.contains('Group|Grouping')) &
           (df['Event name'] != 'Group message sent'), 'Component'] = 'Groups'

    # h5p
    df.loc[df['Component'] == 'H5P Package', 'Component'] = 'H5P'
    df.loc[(df['Event name'].str.contains('Content')) & (df['Component'] == 'System'), 'Component'] = 'H5P'

    # messaging
    df.loc[(df['Event name'].str.contains('(?i)Message')) & (df['Component'] == 'System'), 'Component'] = 'Messaging'
    df.loc[(df['Event name'] == 'Notification sent') &
           (df['Affected user'] != df['User full name']), 'Component'] = 'Messaging'
    mca = list(df.loc[df['Event name'] == 'Message contact added'].index)
    for idx in mca:
        # invert user full name with affected user
        df.loc[idx, ['User full name', 'Affected user']] = df.loc[idx, ['Affected user', 'User full name']].values

    # notes
    df.loc[(df['Event name'].str.contains('(?i)Notes')) & (df['Component'] == 'System'), 'Component'] = 'Notes'

    # notification
    df.loc[df['Event name'] == 'Notification viewed', 'Component'] = 'Notification'

    # profile participant
    df.loc[df['Event name'] == 'User list viewed', 'Component'] = 'Participant profile'
    df.loc[(df['Event name'] == 'User profile viewed') &
           (df['User full name'] != df['Affected user']), 'Component'] = 'Participant profile'

    # profile user
    df.loc[df['Event name'] == 'User password updated', 'Component'] = 'User profile'
    df.loc[df['Event name'] == 'User updated', 'Component'] = 'User profile'
    df.loc[(df['Event name'] == 'User profile viewed') &
           (df['User full name'] == df['Affected user']), 'Component'] = 'User profile'

    # quiz
    df.loc[(df['Event name'].str.contains('Question')) & (df['Component'] == 'System'), 'Component'] = 'Quiz'

    # report
    df.loc[df['Component'] == 'Course participation', 'Component'] = 'Report'
    df.loc[df['Component'] == 'Activity report', 'Component'] = 'Report'
    df.loc[df['Component'] == 'Statistics', 'Component'] = 'Report'

    # role
    df.loc[df['Event name'].str.contains('Role'), 'Component'] = 'Role'

    # tag
    df.loc[(df['Event name'].str.contains('Tag')), 'Component'] = 'Tag'

    # site home
    df.loc[(df['Event context'] == 'Front page') & (df['Event name'] == 'Course viewed'), 'Component'] = 'Site home'

    # web service
    df.loc[df['Course'] == 'Mobile', 'Component'] = 'Web service'

    return df
