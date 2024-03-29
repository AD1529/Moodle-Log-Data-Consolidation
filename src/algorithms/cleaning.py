from pandas import DataFrame
import src.algorithms.integrating as it


def remove_deleted_users(df: DataFrame, deleted_users: str) -> DataFrame:
    """
    Remove records related to deleted users.

    Query for deleted users:
        SELECT id
        FROM mld_user
        WHERE deleted = 1

    Args:
        df (object): The dataframe object.
        deleted_users: The path to deleted users file

    Returns:
        The cleaned dataframe.

    """
    if deleted_users != '':
        # get data
        deleted_users = it.get_dataframe(deleted_users, columns='id')
        # set data types
        deleted_users['id'] = deleted_users['id'].astype('Int64')
        # remove records of deleted users
        for user_id in deleted_users['id']:
            deleted_user_logs = list((df.loc[df['userid'] == user_id]).index)
            df.drop(deleted_user_logs, axis=0, inplace=True)

    return df


def remove_automatic_events(df: DataFrame) -> DataFrame:
    """
    Remove unnecessary data. Here are listed logs that usually do not involve any user actions, rather are
    automatically generated by the system. Please be aware that if you deal with time, and you calculate the duration
    as the interval between two consecutive events, the automatic events must be removed before the duration calculation
    to avoid biased results.
    """

    # automatically generated events that do not involve student actions
    grd_itm_ctd = list((df.loc[df['Role'] == 'Student'].loc[df['Event_name'] == 'Grade item created']).index)
    grd_itm_upd = list((df.loc[df['Role'] == 'Student'].loc[df['Event_name'] == 'Grade item updated']).index)
    user_graded = list((df.loc[df['Role'] == 'Student'].loc[df['Event_name'] == 'User graded']).index)
    notification = list((df.loc[df['Event_name'] == 'Notification sent']).index)
    prediction = list((df.loc[df['Event_name'] == 'Prediction process started']).index)
    cron = list((df.loc[df['Username'] == '-']).index)
    cli = list((df.loc[df['Origin'] == 'cli']).index)
    restore = list((df.loc[df['Origin'] == 'restore']).index)

    to_remove = grd_itm_ctd + grd_itm_upd + user_graded + notification + prediction + cron + cli + restore
    df.drop(to_remove, axis=0, inplace=True)
    df = df.reset_index(drop=True)

    return df
