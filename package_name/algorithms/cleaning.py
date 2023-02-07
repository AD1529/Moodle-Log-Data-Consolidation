from pandas import DataFrame
from package_name.classes.records import Records


def remove_admin_cron_guest_records(df: DataFrame) -> DataFrame:
    # admin activities
    admin = (df.loc[df['Role'] == 'Admin']).index
    cron = (df.loc[df['Username'] == '-']).index
    cli = (df.loc[df['Origin'] == 'cli']).index
    restore = (df.loc[df['Origin'] == 'restore']).index
    guest = (df.loc[df['Role'] == 'Guest']).index

    # drop records
    to_remove = list(admin) + list(cron) + list(cli) + list(restore) + list(guest)
    df.drop(to_remove, axis=0, inplace=True)

    return df


def remove_records_left(df: DataFrame) -> DataFrame:
    """
    Remove data whose course is not provided in the list of courses with id and year and whose area is absent
    Args:
        df:
    Returns:

    """

    to_remove = (df.loc[df.Course_Area == 'nan']).index
    df.drop(to_remove, axis=0, inplace=True)

    return df


def clean_records(records: Records) -> Records:

    """
    This is customisable according to your needs.

    Args:
        records:

    Returns:

    """

    df = records.get_df()

    # cron
    grd_itm_ctd = list((df.loc[df['Role'] == 'Student'].loc[df['Event_name'] == 'Grade item created']).index)
    grd_itm_upd = list((df.loc[df['Role'] == 'Student'].loc[df['Event_name'] == 'Grade item updated']).index)
    user_graded = list((df.loc[df['Role'] == 'Student'].loc[df['Event_name'] == 'User graded']).index)

    # dataset general
    logs = list((df.loc[df['Component'] == 'Logs']).index)
    recycle_bin = list((df.loc[df['Component'] == 'Recycle bin']).index)
    failed_login = list((df.loc[df['Event_name'] == 'User login failed']).index)
    report = list((df.loc[df['Component'] == 'Report']).index)
    other = list((df.loc[df['Component'] == 'Other']).index)
    insights = list((df.loc[df['Event_name'] == 'Insights viewed']).index)
    prediction = list((df.loc[df['Event_name'] == 'Prediction process started']).index)
    mobile = list((df.loc[df['Component'] == 'Web service']).index)
    system = list((df.loc[df['Component'] == 'System']).index)
    login_as = list((df.loc[df['Username'].str.contains(' as ')]).index)

    to_remove = grd_itm_ctd + grd_itm_upd + user_graded + \
        logs + recycle_bin + failed_login + report + other + insights + prediction + mobile + system + login_as
    df.drop(to_remove, axis=0, inplace=True)

    return records


def clean_dataset_records(records: Records) -> Records:

    df = records.get_df()

    # dataset specific
    xp = list((df.loc[df['Component'] == 'Level Up XP']).index)
    wooclap = list((df.loc[df['Component'] == 'Wooclap']).index)
    chat = list((df.loc[df['Component'] == 'Chat']).index)
    reservation = list((df.loc[df['Component'] == 'Reservation']).index)
    activity_completion = list((df.loc[df['Event_name'] == 'Course activity completion updated']).index)
    mod_choice = list((df.loc[df['Component'] == 'mod_choicegroup']).index)
    notification = list((df.loc[(df['Event_name'] == 'Notification sent')
                                & (df['Component'] == 'Assignment')]).index)

    to_remove = xp + wooclap + chat + reservation + activity_completion + mod_choice + notification
    df.drop(to_remove, axis=0, inplace=True)

    return records
