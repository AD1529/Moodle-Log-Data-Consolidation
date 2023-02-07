from pandas import DataFrame
import package_name.algorithms.timing as tm


def rename_columns(df: DataFrame) -> DataFrame:

    # rename columns
    df.rename(columns={'id': 'ID',
                       'timecreated': 'Unix_Time',
                       'User full name': 'Username',
                       'Affected user': 'Affected_user',
                       'Event context': 'Event_context',
                       'courseid': 'course_id',
                       'userid': 'user_id',
                       'relateduserid': 'related_user_id',
                       'Course': 'Course_Area',
                       'IP address': 'IP_address',
                       'Event name': 'Event_name'},
              inplace=True)

    return df


def convert_data_types(df: DataFrame) -> DataFrame:

    # set data types
    df['Time'] = df['Time'].astype('str')
    df['Username'] = df['Username'].astype('str')
    df['Affected_user'] = df['Affected_user'].astype('str')
    df['Event_context'] = df['Event_context'].astype('str')
    df['Component'] = df['Component'].astype('str')
    df['Event_name'] = df['Event_name'].astype('str')
    df['Description'] = df['Description'].astype('str')
    df['Origin'] = df['Origin'].astype('str')
    df['IP_address'] = df['IP_address'].astype('str')
    df['ID'] = df['ID'].astype('Int64')
    df['user_id'] = df['user_id'].astype('Int64').astype('str')
    df['course_id'] = df['course_id'].astype('Int64').astype('str')
    df['related_user_id'] = df['related_user_id'].astype('str')
    df['Unix_Time'] = df['Unix_Time'].astype('Int64')
    df['Course_Area'] = df['Course_Area'].astype('str')
    df['Year'] = df['Year'].astype('str').astype('Int64')
    df['Role'] = df['Role'].astype('str')

    return df


def make_timestamp_readable(df: DataFrame) -> DataFrame:

    df['Time'] = df.loc[:, 'Unix_Time'].map(lambda x: tm.convert_timestamp_to_time(x))

    return df
