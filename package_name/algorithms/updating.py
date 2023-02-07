from package_name.classes.records import Records
from pandas import Series


def update_duration_values(duration_values: [],
                           scenario: str,
                           threshold: int,
                           substitute: int) -> []:
    """
    Verify if the duration values exceed the threshold, and if they do, update their value. Return the list of the
    duration values.

    Args:
        duration_values: list of duration values whose distribution is analysed
        scenario: 'mean_st_dev', 'median_mad', 'iqr_median', ... (algorithms.outliers lists all implemented methods)
        threshold: value that serves as an upper threshold before a value being considered an outlier
        substitute: value that replace the outlier

    Returns:
        List of duration values without outliers
    """

    if scenario == 'winsor' or scenario == 'boot_t_statistic' or scenario == 'boot_mean':
        updated_duration_values = update_lower_upper_duration_values(duration_values, threshold, substitute)
    else:
        # if the value is higher than the threshold, the replacement value is used instead
        updated_duration_values = [substitute if value > threshold else value for value in duration_values]

    return updated_duration_values


def update_lower_upper_duration_values(duration_values: [],
                                       lower_threshold: int,
                                       upper_threshold: int) -> []:
    """
    Verify if the duration values exceed the upper_threshold, and if they do, replace their value with the
    upper_threshold. Verify if the duration values are lower than the lower_threshold, and if they do, replace their
    value with the lower_threshold.

    Args:
        duration_values: list of duration values whose distribution is analysed
        lower_threshold: serves as the lower threshold in the duration values before a value being considered an outlier
        upper_threshold: serves as the upper threshold in the duration values before a value being considered an outlier

    Returns:
        List of duration values without outliers
    """

    # the upper threshold replaces the value if it is higher; the lower threshold replaces the value if it is lower
    updated_duration_values = [upper_threshold if value > upper_threshold
                               else lower_threshold if value < lower_threshold else value for value in duration_values]

    return updated_duration_values


def add_updated_duration_values_to_dataframe(records: Records,
                                             selected_records: Series,
                                             updated_duration_values: [],
                                             updated_duration_field: str = 'updated_duration') -> Records:
    """
    Add the updated duration values to the object Records.

    Args:
        records: records to analyse
        selected_records: values filtered based on the selected columns and to update in the dataframe
        updated_duration_values: new duration values used to replace the old ones
        updated_duration_field: name of the column in the dataframe

    Returns:
        The updated object Records
    """

    # get the dataframe
    df = records.get_df()

    # find the selected records' index in the dataframe
    index = selected_records.index
    # add a new column with the updated duration values to the dataframe
    df.loc[index, updated_duration_field] = updated_duration_values

    return records
