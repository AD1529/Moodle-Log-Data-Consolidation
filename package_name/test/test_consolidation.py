from package_name.execution.paths import *
from package_name.classes.records import Records
import package_name.algorithms.cleaning as cl
import package_name.algorithms.extracting as ex
import package_name.execution.consolidate_dataframe as cd
import package_name.algorithms.sorting as st

# ------------------------------
# GET THE CONSOLIDATED DATAFRAME
# ------------------------------
data_frame = cd.get_consolidated_logs(platform_logs,
                                      database_logs,
                                      course_shortnames_path,
                                      course_students_path,
                                      course_teachers_path,
                                      course_non_editing_teachers_path,
                                      managers_path)


# -------------------------------------
# PREPARE DATA
# -------------------------------------
# create a Records object to use its methods
records = Records(data_frame)
# to calculate the duration values must be sorted by username and ID
records = st.sort_records(records, sort_by=['Username', 'ID'])
# clean useless data
records = cl.clean_records(records)

# -----------------------------------------
# CLEAN THE COMPLETE DATASET
# -----------------------------------------
records = cl.clean_dataset_records(records)
df = records.get_df()
# df.to_csv('../datasets/data_consolidation/df_prepared.csv')


"""
# -----------------------
# SELECT SPECIFIC COURSES
# -----------------------
# select specific attributes to get the desired values
course_A_2021_records = ex.extract_records(records, [2021], ['ALL-DA'], ['Student'], filepath=course_dates_path)
course_B_2021_records = ex.extract_records(records, [2021], ['ALL-DB'], ['Student'], filepath=course_dates_path)
course_C_2021_records = ex.extract_records(records, [2021], ['DMML'], ['Student'], filepath=course_dates_path)
course_D_2021_records = ex.extract_records(records, [2021], ['ICT'], ['Student'], filepath=course_dates_path)
course_A_2022_records = ex.extract_records(records, [2022], ['ALL-DA'], ['Student'], filepath=course_dates_path)
course_B_2022_records = ex.extract_records(records, [2022], ['ALL-DB'], ['Student'], filepath=course_dates_path)
course_C_2022_records = ex.extract_records(records, [2022], ['DMML'], ['Student'], filepath=course_dates_path)
course_D_2022_records = ex.extract_records(records, [2022], ['ICT'], ['Student'], filepath=course_dates_path)


# --------------------------
# CLEAN THE SPECIFIC DATASET
# --------------------------
course_A_2021_records = cl.clean_dataset_records(course_A_2021_records)
course_B_2021_records = cl.clean_dataset_records(course_B_2021_records)
course_C_2021_records = cl.clean_dataset_records(course_C_2021_records)
course_D_2021_records = cl.clean_dataset_records(course_D_2021_records)
course_A_2022_records = cl.clean_dataset_records(course_A_2022_records)
course_B_2022_records = cl.clean_dataset_records(course_B_2022_records)
course_C_2022_records = cl.clean_dataset_records(course_C_2022_records)
course_D_2022_records = cl.clean_dataset_records(course_D_2022_records)
"""