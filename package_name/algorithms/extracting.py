from package_name.classes.records import Records
import package_name.algorithms.integrating as it
from pandas import DataFrame


def get_startdate_enddate(df: DataFrame, file_path: str, courses: [str], years: [int]):
    # remove values which are out of bounds wrt to start and end date
    # in 2021 students accessed courses before their starting time
    # the collection of data ceases on the day of the exam

    """
        select id, shortname as course_name, startdate as start_date, enddate as end_date
        from prefix_course
        where id <> 1

        Returns:

    """
    course_dates = it.get_dataframe(file_path, columns=['id', 'shortname', 'startdate', 'enddate'])

    for course in courses:
        for year in years:
            shortname = course + '_' + str(year)
            startdate = course_dates.loc[course_dates['shortname'] == shortname, 'startdate'].values[0]
            enddate = course_dates.loc[course_dates['shortname'] == shortname, 'enddate'].values[0]
            to_remove = (df.loc[(df.Course_Area == course) & (df.Year == year) &
                                (df.Unix_Time < startdate) | (df.Unix_Time > enddate)]).index
            df.drop(to_remove, axis=0, inplace=True)

    return df


def extract_records(records: Records,
                    year: [int] = None,
                    course_area: [str] = None,
                    role: [str] = None,
                    username: [str] = None,
                    filepath: str = "") -> Records:

    """
    Return the filtered records by year, course_area, role and/or username and sorted by the specified field.

    Args:
        records: object of the class Records to analyse
        year: year of the course/area
        course_area: course(s) or area(s) of the platform
        role: 'Student', 'Teacher', 'Manager', etc.
        username: user id's
        filepath:

    Returns:
        Object of the class Records
    """

    # attributes to filter
    filters = dict([('Year', year),
                    ('Course_Area', course_area),
                    ('Role', role),
                    ('Username', username)
                    ])

    # columns in the dataframe
    columns = [i for i in filters if filters[i] is not None]

    # get the df
    df = records.get_df()

    for column in columns:
        # for each column filter the values
        df = df.loc[df[column].isin(filters.get(column))]

    # get only the values between start_date and end_date
    if filepath != "":
        df = get_startdate_enddate(df, filepath, course_area, year)

    # create a Records object for the extracted values
    records = Records(df)

    return records
