from pandas import DataFrame
import src.algorithms.timing as tm


def rename_columns(df: DataFrame) -> DataFrame:
    """
    Rename the colum names.

    Args:
        df: The dataframe object.

    Returns:
        The dataframe with renamed columns.

    """

    # rename columns
    df.rename(columns={'User full name': 'Username',
                       'Affected user': 'Affected_user',
                       'Event context': 'Event_context',
                       'Event name': 'Event_name',
                       'IP address': 'IP_address',
                       'id': 'ID',
                       'timecreated': 'Unix_Time'},
              inplace=True)

    return df


def make_timestamp_readable(df: DataFrame) -> DataFrame:
    """
    Transform the timestamps in standard format.

    Args:
        df: The dataframe object.

    Returns:
        The dataframe with a new column 'Time'.

    """

    df['Time'] = df.loc[:, 'Unix_Time'].map(lambda x: tm.convert_timestamp_to_time(x))

    return df
