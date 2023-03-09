from pandas import DataFrame
from src.classes.records import Records
import src.algorithms.integrating as it


def remove_admin_cron_guest_records(df: DataFrame) -> DataFrame:
    """
    Remove admin, cron and guest activities.
    ‘Restore’: typically teachers and managers activities related to role permission.
    ‘CLI’: events triggered by a CLI script unrelated to user action
    '-': cron jobs.

    Args:
        df: The dataframe object.

    Returns:
        The cleaned dataframe.

    """
    # admin activities
    admin = list((df.loc[df['Role'] == 'Admin']).index)
    cron = list((df.loc[df['Username'] == '-']).index)
    cli = list((df.loc[df['Origin'] == 'cli']).index)
    restore = list((df.loc[df['Origin'] == 'restore']).index)
    guest = list((df.loc[df['Role'] == 'Guest']).index)
    login_as = list((df.loc[(df['Username'].str.contains(' as ')) & (df['Event_context'] == 'Other')]).index)

    # drop records
    to_remove = admin + cron + cli + restore + guest + login_as
    df.drop(to_remove, axis=0, inplace=True)

    return df


def remove_deleted_users(df: DataFrame, deleted_users: str) -> DataFrame:
    """
    Remove records related to deleted users.

    Query for deleted users:
        SELECT id
        FROM mld_user
        WHERE deleted = 1

    Args:
        df: The dataframe object.
        deleted_users: str,
            The path to

    Returns:
        The cleaned dataframe.

    """
    if deleted_users != '':
        deleted_users = it.get_dataframe(deleted_users, columns='id')
        for user_id in deleted_users['id']:
            user_logs = list((df.loc[df['user_id'] == str(user_id)]).index)
            df.drop(user_logs, axis=0, inplace=True)

    return df


def clean_dataset_records(df: DataFrame) -> DataFrame:
    """
    Remove unnecessary data. Here we list logs that usually do not involve any user actions or are not related to
    learning activities. Moreover, some user logs may be gathered for deleted courses or for courses not listed in the
    course_shortnames file (because you do not want to analyse them). When attempting to match the course id and
    shortname, they are not be matched and the course/area field will be empty. This function is customisable according
    to specific needs.

    Please be aware that if you deal with time, and you calculate the duration as the interval between two
    consecutive events, the dataset cleaning should be performed after the duration calculation to avoid biased results.

    Args:
        df: The dataframe object.

    Returns:
        The cleaned dataframe.

    """

    # automatically generated events that do not involve student actions
    grd_itm_ctd = list((df.loc[df['Role'] == 'Student'].loc[df['Event_name'] == 'Grade item created']).index)
    grd_itm_upd = list((df.loc[df['Role'] == 'Student'].loc[df['Event_name'] == 'Grade item updated']).index)
    user_graded = list((df.loc[df['Role'] == 'Student'].loc[df['Event_name'] == 'User graded']).index)

    # logs not related to learning activities
    logs = list((df.loc[df['Component'] == 'Logs']).index)
    recycle_bin = list((df.loc[df['Component'] == 'Recycle bin']).index)
    failed_login = list((df.loc[df['Event_name'] == 'User login failed']).index)
    report = list((df.loc[df['Component'] == 'Report']).index)
    insights = list((df.loc[df['Event_name'] == 'Insights viewed']).index)
    prediction = list((df.loc[df['Event_name'] == 'Prediction process started']).index)

    # actions performed on deleted modules
    other = list((df.loc[df['Event_context'] == 'Other']).index)

    # data whose course is not listed in the course_shortnames file and whose area is absent
    records_left = (df.loc[df.Course_Area == 'nan']).index  # the course/area field if of type text

    # remove remaining admin/manager-related actions that are not specified in the integrating functions
    system = list((df.loc[df['Component'] == 'System']).index)

    to_remove = grd_itm_ctd + grd_itm_upd + user_graded + \
        logs + recycle_bin + failed_login + report + insights + prediction + other + records_left + system
    df.drop(to_remove, axis=0, inplace=True)

    return df


def clean_records(records: Records) -> Records:
    """
    This function removes specific records. This function is customisable according to specific needs.

    Args:
        records: The Records object.

    Returns:
        The cleaned Records object.

    """

    df = records.get_df()

    # dataset specific
    xp = list((df.loc[df['Component'] == 'Level Up XP']).index)
    wooclap = list((df.loc[df['Component'] == 'Wooclap']).index)
    chat = list((df.loc[df['Component'] == 'Chat']).index)
    reservation = list((df.loc[df['Component'] == 'Reservation']).index)
    activity_completion = list((df.loc[df['Event_name'] == 'Course activity completion updated']).index)
    mod_choice = list((df.loc[df['Component'] == 'mod_choicegroup']).index)
    notification = list((df.loc[(df['Event_name'] == 'Notification sent') &
                                ((df['Component'] == 'Assignment') | (df['Component'] == 'Notification'))]).index)

    to_remove = xp + wooclap + chat + reservation + activity_completion + mod_choice + notification
    df.drop(to_remove, axis=0, inplace=True)

    return records
