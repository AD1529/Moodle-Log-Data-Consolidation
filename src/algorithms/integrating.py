import src.algorithms.transforming as tr
import pandas as pd
from pandas import DataFrame
import glob


def get_dataframe(file_path: str, columns: [] = None) -> DataFrame:
    """
    Read the dataframe and add columns if missing.

    Args:
        file_path: str,
            The path of the dataframe object.
        columns: list,
            The list of column names.

    Returns:
        The dataframe with column names.

    """

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
    It is possible to choose specific users (within one or more courses) and download their logs if the memory size
    of the Moodle platform does not permit the download of the entire set of logs. Then, a directory should contain all
    files.

    Args:
        directory_path: str,
            the path of the directory that contains all the logs files.

    Returns:
        The dataframe containing the logs of the selected students.

    """

    global_table = pd.DataFrame()

    for file_path in glob.glob(directory_path + '*.csv'):
        # get the user logs
        user_logs = get_dataframe(file_path)
        # concatenate the user logs to the global table
        global_table = pd.concat([global_table, user_logs], axis=0)

    # reset the index
    global_table = global_table.reset_index(drop=True)

    return global_table


def get_joined_logs(platform: str or DataFrame, database: str) -> DataFrame:
    """
    First collect the site logs extracted from the Moodle log generation interface that is available in every Moodle
    platform (https://your_moodle_site/report/log/index.php?id=0), then the database logs extracted from the table
    'mdl_logstore_standard_log'.
    Timestamps are reversed since the first log identifies the first action recorded, whereas the first log extracted
    from Moodle log generation interface represents the last action. Some values may not be merged because the lengths
    of the two files may not be equal (some logs can be missed if you download the two files one after the other) ,
    thus they must be aligned. You can either use the dataframe or a filepath.
    Please note that if you want to select specific users, the query for the extraction of database data must be
    filtered with their specific userid.

    Query to extract database data:
        SELECT id, userid, courseid, relateduserid, timecreated
        FROM mdl_logstore_standard_log
        # WHERE userid = ???

    Args:
        platform: str,
            The logs dataframe path or Dataframe.
            platform_logs columns: ['Time', 'User full name', 'Affected user', 'Event context', 'Component',
                                    'Event name', 'Description', 'Origin', 'IP address']

        database: str,
            The path of the data extracted from the database.
            database_logs columns: ['id', 'userid', 'courseid', 'relateduserid', 'timecreated']

    Returns:
        The dataset that integrates platform and database logs.

    """

    if isinstance(platform, str):
        # import the complete set of logs extracted from Moodle log extraction interface
        platform_logs = get_dataframe(platform)
    else:
        # if the parameter is a DataFrame
        platform_logs = platform

    # import the logs extracted from database
    database_data = get_dataframe(database, columns=['id', 'userid', 'courseid', 'relateduserid', 'timecreated'])

    # reverse platform logs
    platform_logs = platform_logs[::-1].copy()
    platform_logs = platform_logs.reset_index(drop=True)

    # sort database data
    database_data = database_data.sort_values(by=['timecreated', 'id'])
    database_data = database_data.reset_index(drop=True)

    # align the number of records for both dataframes
    if len(platform_logs) != len(database_data):
        n = abs(len(database_data) - len(platform_logs))
        # drop the exceeding rows
        if len(database_data) > len(platform_logs):
            to_drop = list(database_data.tail(n).index)
            database_data = database_data.drop(to_drop)
        else:
            to_drop = list(platform_logs.tail(n).index)
            platform_logs = platform_logs.drop(to_drop)

    # concatenate data
    joined_logs = pd.concat([platform_logs, database_data], axis=1)

    # rename the dataframe columns
    joined_logs = tr.rename_columns(joined_logs)
    # set data type for further analysis
    joined_logs = tr.set_data_types(joined_logs)

    return joined_logs


def add_course_shortname(df: DataFrame, course_names: str) -> DataFrame:
    """
    Add the shortname of the course based on the courseid extracted from the table 'mdl_course'. By modifying the
    function, it is also possible to add the fullname according to specific requirements. Please be aware that if you
    want to retrieve data only for specific courses, you can list them in the course names file (with the corresponding
    id). Data belonging to other courses will be removed during cleaning.

    Query: SELECT id, shortname (or fullname)
           FROM mdl_course

    Args:
        df: The dataframe object.
        course_names: str,
            The path of the data extracted from the database.

    Returns:
        The dataframe with the field shortname.

    """
    # get data
    course_names = get_dataframe(course_names, columns=['id', 'shortname'])
    # set data type
    course_names['id'] = course_names['id'].astype('Int64')
    course_names['shortname'] = course_names['shortname'].astype('str')
    shortnames = course_names['shortname']

    for shortname in shortnames:
        courseid = course_names.loc[course_names['shortname'] == shortname]['id'].values[0]
        df.loc[df['courseid'] == courseid, 'Course_Area'] = shortname

    # set data type
    df['Course_Area'] = df['Course_Area'].astype('str')

    return df


def add_year(df: DataFrame) -> DataFrame:
    """
    Add the field year to the dataframe.

    Args:
        df: The dataframe object.

    Returns:
        The joined dataframe with the year field.

    """

    df['Year'] = df['Time'].map(lambda x: int(x.split('/')[2].split(',')[0]) + 2000)

    # set data type
    df['Year'] = df['Year'].astype('Int64')

    return df


def add_role(df: DataFrame,
             student_role: str,
             teacher_role: str = '',
             non_editing_teacher_role: str = '',
             course_creator_role: str = '',
             manager_role: str = '',
             admin_role: str = '') -> DataFrame:

    """
    Add roles to the dataframe.

    A role is a collection of permissions defined for the whole system that can be assigned to specific users in
    specific contexts. When a user logs in, they are considered "authenticated." Users can be teachers or students
    only within a course. A user can have multiple roles, representing as both a teacher and a student in different
    courses. The complete list of roles is available at the page: your_moodle_site/admin/roles/manage.php. This
    function can be extended according to specific requirements.

    Please be aware that any system roles (suche as admin, manager, course-creator, or specifically created role) apply
    to the assigned users throughout the entire system, including the front page and all the courses. A user can be a
    teacher in a course and a student in another course. A manager can only be a manager.

    Query for student, teacher, and non-editing teacher:
        SELECT cx.instanceid as courseid, u.id as userid
        FROM mdl_course c LEFT OUTER JOIN mdl_context cx ON c.id = cx.instanceid
        LEFT OUTER JOIN mdl_role_assignments ra ON cx.id = ra.contextid AND ra.roleid = '???' AND cx.instanceid <> 1
        LEFT OUTER JOIN mdl_user u ON ra.userid = u.id Where cx.contextlevel = '50'

    Query for manager and course creator:
        SELECT distinct userid
        FROM mdl_role_assignments
        WHERE roleid = '???'

    Query for admin:
        SELECT value
        FROM mdl_config
        WHERE name = 'siteadmins'

    Args:
        df: the joined dataframe.
        student_role: str,
            The path of the data extracted from database.
            role id = 5, this field is mandatory
        teacher_role: str,
            The path of the data extracted from database.
            role id = 3
        non_editing_teacher_role: str,
            The path of the data extracted from database.
            role id = 4
        course_creator_role: str,
            The path of the data extracted from database.
            role id = 2
        manager_role: str,
            The path of the data extracted from database.
            role id = 1
        admin_role: str,
            The path of the data extracted from database.

    Returns:
        The joined dataframe with the role integration.

    """

    # get data
    student_role = get_dataframe(student_role, columns=['courseid', 'userid'])
    # set data type
    student_role['courseid'] = student_role['courseid'].astype('Int64')
    student_role['userid'] = student_role['userid'].astype('Int64')
    # assign the course student role by matching the course id and the user id
    for idx in range(len(student_role)):
        df.loc[(df['courseid'] == student_role.iloc[idx]['courseid']) &
               (df['userid'] == student_role.iloc[idx]['userid']), 'Role'] = 'Student'

    if teacher_role != '':
        # get data
        teacher_role = get_dataframe(teacher_role, columns=['courseid', 'userid'])
        # set data type
        teacher_role['courseid'] = teacher_role['courseid'].astype('Int64')
        teacher_role['userid'] = teacher_role['userid'].astype('Int64')
        # assign the course teacher role by matching the course id and the user id
        for idx in range(len(teacher_role)):
            df.loc[(df['courseid'] == teacher_role.iloc[idx]['courseid']) &
                   (df['userid'] == teacher_role.iloc[idx]['userid']), 'Role'] = 'Teacher'

    if non_editing_teacher_role != '':
        # get data
        non_editing_teacher_role = get_dataframe(non_editing_teacher_role, columns=['courseid', 'userid'])
        # set data type
        non_editing_teacher_role['courseid'] = non_editing_teacher_role['courseid'].astype('Int64')
        non_editing_teacher_role['userid'] = non_editing_teacher_role['userid'].astype('Int64')
        #  assign the course non-editing teacher role by matching the course id and the user id
        for idx in range(len(non_editing_teacher_role)):
            df.loc[(df['courseid'] == non_editing_teacher_role.iloc[idx]['courseid']) &
                   (df['userid'] == non_editing_teacher_role.iloc[idx]['userid']), 'Role'] = 'Non-editing Teacher'

    #  assign the course creator role
    if course_creator_role != '':
        # get data
        course_creator_role = get_dataframe(course_creator_role, columns=['userid'])
        # set data type
        course_creator_role['userid'] = course_creator_role['userid'].astype('Int64')
        for idx in range(len(course_creator_role)):
            df.loc[df['userid'] == course_creator_role.iloc[idx]['userid'], 'Role'] = 'Course creator'

    if manager_role != '':
        # get data
        manager_role = get_dataframe(manager_role, columns=['userid'])
        # set data type
        manager_role['userid'] = manager_role['userid'].astype('Int64')
        # assign the manager role
        for idx in range(len(manager_role)):
            df.loc[df['userid'] == manager_role.iloc[idx]['userid'], 'Role'] = 'Manager'

    if admin_role != '':
        # get data
        admin_role = get_dataframe(admin_role, columns=['value'])
        # get userid whose role is admin
        admin_role = admin_role['value'][0].split(',')
        # assign the admin role
        for idx in admin_role:
            df.loc[df['userid'] == int(idx), 'Role'] = 'Admin'

    # assign the role 'guest' to guests and users who access the course just to have a look and then unenroll
    df.loc[df['userid'] == 1, 'Role'] = 'Guest'
    df.loc[(df['Role'].isnull()) &
           (df['Course_Area'].notnull()) &
           (df['Username'] != '-'), 'Role'] = 'Guest'

    # assign the authenticated user to left records
    df.loc[(df['Role'].isnull()) &
           (df['Username'] != '-'), 'Role'] = 'Authenticated user'

    # set data type
    df['Role'] = df['Role'].astype('str')

    return df


def course_area_categorisation(df: DataFrame) -> DataFrame:
    """
    Add the area to those records that do not belong to any field and have a missing value.

    Args:
        df: the joined dataframe.

    Returns:
        The dataframe with the integrated course_area field.

    """

    # authentication
    df.loc[df['Event_name'] == 'User has logged in', 'Course_Area'] = 'Authentication'
    df.loc[df['Event_name'] == 'User login failed', 'Course_Area'] = 'Authentication'
    df.loc[df['Event_name'] == 'User logged out', 'Course_Area'] = 'Authentication'

    # mobile
    df.loc[df['Event_name'].str.contains('Web service'), 'Course_Area'] = 'Mobile'

    # overall site
    df.loc[df['Event_name'].str.contains('Dashboard'), 'Course_Area'] = 'Overall Site'
    df.loc[df['Event_context'].str.contains('(?i)Category'), 'Course_Area'] = 'Overall Site'
    df.loc[df['Event_name'].str.contains('(?i)Category'), 'Course_Area'] = 'Overall Site'
    df.loc[df['Event_name'].str.contains('Calendar'), 'Course_Area'] = 'Overall Site'
    df.loc[df['Event_name'] == 'Courses searched', 'Course_Area'] = 'Overall Site'
    df.loc[df['Event_context'] == 'Front page', 'Course_Area'] = 'Overall Site'
    df.loc[df['Event_context'] == 'Forum: Site announcements', 'Course_Area'] = 'Overall Site'
    df.loc[df['Event_name'] == 'Notification viewed', 'Course_Area'] = 'Overall Site'
    df.loc[(df['Event_name'] == 'Notification sent') &
           (df['Affected_user'] == df['Username']), 'Course_Area'] = 'Overall Site'
    df.loc[(df['Event_name'] == 'Blog entries viewed') &
           (df['Affected_user'] == df['Username']), 'Course_Area'] = 'Overall Site'
    df.loc[(df['Event_name'] == 'User report viewed') &
           (df['Affected_user'] == df['Username']) & (df['Component'] == 'Forum'), 'Course_Area'] = 'Overall Site'
    df.loc[df['Event_context'] == 'Forum: Site announcements', 'Course_Area'] = 'Overall Site'

    # profile
    df.loc[df['Event_name'].str.contains('User password'), 'Course_Area'] = 'Profile'
    df.loc[df['Event_name'] == 'User updated', 'Course_Area'] = 'Profile'
    df.loc[(df['Event_name'] == 'User profile viewed') &
           (df['Affected_user'] == df['Username']), 'Course_Area'] = 'Profile'
    df.loc[(df['Event_name'] == 'Badge viewed') & (df['Event_context'] == 'System'), 'Course_Area'] = 'Profile'
    df.loc[(df['Event_name'] == 'Tag added to an item') &
           (df['Event_context'].str.contains('User:')), 'Course_Area'] = 'Profile'
    df.loc[(df['Event_name'] == 'Tag removed from an item') &
           (df['Event_context'].str.contains('User:')), 'Course_Area'] = 'Profile'
    df.loc[df['Event_name'] == 'Tag created', 'Course_Area'] = 'Profile'
    df.loc[df['Event_name'] == 'Tag deleted', 'Course_Area'] = 'Profile'
    df.loc[(df['Event_name'] == 'Course user report viewed') &
           (df['Affected_user'] == df['Username']), 'Course_Area'] = 'Profile'
    df.loc[(df['Event_name'] == 'Notes viewed') &
           (df['Affected_user'] == df['Username']), 'Course_Area'] = 'Profile'

    # social interaction
    df.loc[(df['Event_name'].str.contains('(?i)message')) &
           (df['Component'] != 'Chat'), 'Course_Area'] = 'Social interaction'
    df.loc[(df['Event_name'] == 'Notification sent') &
           (df['Affected_user'] != df['Username']), 'Course_Area'] = 'Social interaction'
    df.loc[(df['Event_name'] == 'User profile viewed') &
           (df['Affected_user'] != df['Username']), 'Course_Area'] = 'Social interaction'
    df.loc[(df['Event_name'] == 'Blog entries viewed') &
           (df['Affected_user'] != df['Username']), 'Course_Area'] = 'Social interaction'

    return df


def component_redefinition(df: DataFrame) -> DataFrame:
    """
    The component field can be labelled with the 'System' value even though the log is clearly generated when the user
    is performing an action on a specific module. Sometimes some records are recorded on different components even
    though they are related to the same component. This function redefines the component field.
    Reference paper: D Rotelli, A Monreale, Time-on-task estimation by data-driven outlier detection based on learning
    activities, LAK22.

    Args:
        df: the joined dataframe.

    Returns:
        The dataframe with redefined values for the component field.

    """

    # assignment
    df.loc[df['Component'].str.contains('(?i)submission'), 'Component'] = 'Assignment'

    # authentication
    df.loc[df['Event_name'] == 'User has logged in', 'Component'] = 'Login'
    df.loc[df['Event_name'] == 'User login failed', 'Component'] = 'Login'
    df.loc[df['Event_name'] == 'User logged out', 'Component'] = 'Logout'

    # backup
    df.loc[df['Component'].str.contains('backup'), 'Component'] = 'Backup'

    # badge
    df.loc[df['Event_name'].str.contains('Badge'), 'Component'] = 'Badge'

    # blog
    df.loc[df['Event_name'].str.contains('Blog'), 'Component'] = 'Blog'

    # book
    df.loc[df['Component'] == 'Book printing', 'Component'] = 'Book'

    # calendar
    df.loc[df['Event_name'].str.contains('Calendar'), 'Component'] = 'Calendar'

    # capability
    df.loc[df['Event_name'].str.contains('Capability'), 'Component'] = 'Capability'

    # course activity completion updated
    ccu = list(df.loc[df['Event_name'] == 'Course activity completion updated'].index)
    for idx in ccu:
        df.loc[idx, 'Component'] = df.loc[idx, 'Event_context'].split(':')[0]

    # course home
    df.loc[df['Event_name'].str.contains('Course section'), 'Component'] = 'Course home'
    df.loc[(df['Event_context'].str.contains('Course_Area')) &
           (df['Event_name'] == 'Course viewed'), 'Component'] = 'Course home'

    # course module created
    cmc = list(df.loc[df['Event_name'] == 'Course module created'].index)
    for idx in cmc:
        df.loc[idx, 'Component'] = df.loc[idx, 'Event_context'].split(':')[0]

    # course module updated
    cmu = list(df.loc[df['Event_name'] == 'Course module updated'].index)
    for idx in cmu:
        df.loc[idx, 'Component'] = df.loc[idx, 'Event_context'].split(':')[0]

    # courses list
    df.loc[df['Event_name'] == 'Category viewed', 'Component'] = 'Courses list'
    df.loc[df['Event_name'] == 'Courses searched', 'Component'] = 'Courses list'

    # dashboard
    df.loc[df['Event_name'].str.contains('Dashboard'), 'Component'] = 'Dashboard'

    # enrollment
    df.loc[df['Event_name'] == 'User enrolled in course', 'Component'] = 'Enrollment'
    df.loc[df['Event_name'] == 'User unenrolled from course', 'Component'] = 'Enrollment'

    # grade-book
    df.loc[df['Component'] == 'Single view', 'Component'] = 'Gradebook'
    df.loc[df['Component'] == 'Excel spreadsheet', 'Component'] = 'Gradebook'
    df.loc[df['Component'] == 'OpenDocument spreadsheet', 'Component'] = 'Gradebook'
    df.loc[df['Component'] == 'Grader report', 'Component'] = 'Gradebook'
    df.loc[df['Component'] == 'Outcomes report', 'Component'] = 'Gradebook'
    df.loc[df['Event_name'] == 'User graded', 'Component'] = 'Gradebook'
    df.loc[df['Event_name'] == 'Grade item updated', 'Component'] = 'Gradebook'
    df.loc[df['Event_name'] == 'Grade deleted', 'Component'] = 'Gradebook'
    df.loc[df['Event_name'] == 'Grade item created', 'Component'] = 'Gradebook'
    df.loc[df['Event_name'] == 'Scale created', 'Component'] = 'Gradebook'
    df.loc[df['Event_name'] == 'Scale deleted', 'Component'] = 'Gradebook'

    # grades
    df.loc[df['Event_name'] == 'Grade overview report viewed', 'Component'] = 'Grades'
    df.loc[df['Event_name'] == 'Course user report viewed', 'Component'] = 'Grades'
    df.loc[df['Component'] == 'User report', 'Component'] = 'Grades'

    # groups
    df.loc[(df['Event_name'].str.contains('Group|Grouping')) &
           (df['Event_name'] != 'Group message sent'), 'Component'] = 'Groups'

    # h5p
    df.loc[df['Component'] == 'H5P Package', 'Component'] = 'H5P'
    df.loc[(df['Event_name'].str.contains('Content')) & (df['Component'] == 'System'), 'Component'] = 'H5P'

    # messaging
    df.loc[(df['Event_name'].str.contains('(?i)Message')) & (df['Component'] == 'System'), 'Component'] = 'Messaging'
    df.loc[(df['Event_name'] == 'Notification sent') &
           (df['Affected_user'] != df['Username']), 'Component'] = 'Messaging'
    mca = list(df.loc[df['Event_name'] == 'Message contact added'].index)
    for idx in mca:
        # invert username with affected user
        df.loc[idx, ['Username', 'Affected_user']] = df.loc[idx, ['Affected_user', 'Username']].values

    # notes
    df.loc[(df['Event_name'].str.contains('(?i)Notes')) & (df['Component'] == 'System'), 'Component'] = 'Notes'

    # notification
    df.loc[df['Event_name'] == 'Notification viewed', 'Component'] = 'Notification'
    df.loc[df['Event_name'] == 'Notification sent', 'Component'] = 'Notification'

    # profile participant
    df.loc[df['Event_name'] == 'User list viewed', 'Component'] = 'Participant profile'
    df.loc[(df['Event_name'] == 'User profile viewed') &
           (df['Username'] != df['Affected_user']), 'Component'] = 'Participant profile'

    # profile user
    df.loc[df['Event_name'] == 'User password updated', 'Component'] = 'User profile'
    df.loc[df['Event_name'] == 'User updated', 'Component'] = 'User profile'
    df.loc[(df['Event_name'] == 'User profile viewed') &
           (df['Username'] == df['Affected_user']), 'Component'] = 'User profile'

    # quiz
    df.loc[(df['Event_name'].str.contains('Question')) & (df['Component'] == 'System'), 'Component'] = 'Quiz'

    # report
    df.loc[df['Component'] == 'Course participation', 'Component'] = 'Report'
    df.loc[df['Component'] == 'Activity report', 'Component'] = 'Report'
    df.loc[df['Component'] == 'Statistics', 'Component'] = 'Report'

    # role
    df.loc[df['Event_name'].str.contains('Role'), 'Component'] = 'Role'

    # tag
    df.loc[(df['Event_name'].str.contains('Tag')), 'Component'] = 'Tag'

    # site home
    df.loc[(df['Event_context'] == 'Front page') & (df['Event_name'] == 'Course viewed'), 'Component'] = 'Site home'

    # web service
    df.loc[df['Course_Area'] == 'Mobile', 'Component'] = 'Web service'

    return df


def identify_deleted_modules(df: DataFrame) -> DataFrame:
    """
    Identify actions performed on deleted modules

    Args:
        df: The dataframe object.

    Returns:
        The dataframe with the type values for the deleted modules.

    """

    df.loc[df['Event_context'] == 'Other', 'Type'] = 'Deleted'
    df.loc[df['Type'].isnull(), 'Type'] = 'Available'

    # set data type
    df['Type'] = df['Type'].astype('str')

    return df
