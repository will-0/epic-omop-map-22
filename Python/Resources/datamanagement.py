import pandas as pd
import os
from stat import S_IREAD, S_IRGRP, S_IROTH, S_IWUSR, S_IREAD
from datetime import datetime

def get_eldef():
    return pd.read_csv("Resources/__ReadOnly/__ElementDefinitions.csv")[["examArea","dataElement","CUI"]]\
        .astype({"examArea":"string", "dataElement":"string", "CUI":"string"})

def get_valdef():
    df_valdef = pd.read_csv("Resources/__ReadOnly/__ValueDefinitions.csv")\
        .astype({"CUI":"string", "value":"string", "valid":"bool"})
    return df_valdef.loc[df_valdef.valid][["ID", "CUI", "value"]]\
        .astype({"ID":"int64", "CUI":"string", "value":"string"})

def get_origindex():
    return pd.read_csv("Resources/__ReadOnly/__OrigIndex.csv")\
        .astype({"CUI":"string", "orig_index":"int"})

def valuedef_update(return_updated_df=False):
    persistent_file_path = "Resources/__ReadOnly/__ValueDefinitions.csv"

    # Pull the current dataframe
    df_valdef_persistent = pd.read_csv(persistent_file_path, index_col="ID")\
        .astype({"CUI":"string", "value":"string", "valid":"bool", "creation_date":"string", "invalid_date":"string"})

    directory = "Resources/ValueDefinitions/"
    df_list = []
    print("Scanning value definition files in \"%s\"" % directory)
    file_count = 0
    for filename in os.listdir("Resources/ValueDefinitions/"):
        file_count += 1
        test_df = pd.read_csv(directory+filename).astype({"CUI":"string", "value":"string"})[["CUI", "value"]]
        ind_new_val = pd.Index([])
        for cui in test_df.CUI.unique():
            # Get the indices of new values for this CUI
            ind_new_val_for_cui = test_df.loc[(test_df.CUI == cui) \
                & (~test_df.value.isin(df_valdef_persistent.loc[df_valdef_persistent.CUI == cui]["value"]))]\
                    .index
            # Add this to the index list
            ind_new_val = ind_new_val.union(ind_new_val_for_cui)
        # Add new row subset to dataframe list
        df_list.append(test_df.iloc[ind_new_val])
    print("Scanned %d file(s)" % file_count)

    # Concatenate DataFrames
    df_newrows = pd.concat(df_list, ignore_index=True).drop_duplicates()
    n_new_rows = df_newrows.shape[0]
    print("Found %d new value entries" % n_new_rows)

    # Add the validity and creation date fields
    df_newrows["valid"] = True
    df_newrows["creation_date"] = datetime.now().date()
    df_newrows["invalid_date"] = None
    df_newrows = df_newrows.astype({"CUI":"string", "value":"string", "valid":"bool", "creation_date":"string", "invalid_date":"string"})

    # Append to the DataFrame list
    prev_index = df_valdef_persistent.index
    df_valdef_new = pd.concat([df_valdef_persistent, df_newrows], ignore_index=True)
    assert df_valdef_new.iloc[prev_index].equals(df_valdef_persistent)

    if n_new_rows > 0:
        try:
            os.chmod(persistent_file_path, S_IWUSR|S_IREAD)
            df_valdef_new.to_csv(persistent_file_path, index_label="ID")
            print("UPDATED \"%s\"" % persistent_file_path)
            os.chmod(persistent_file_path, S_IREAD|S_IRGRP|S_IROTH)
            print("ID range for new rows: [%d..%d]" % (prev_index.max() + 1, df_valdef_new.index.max()))
            # Update the usable definitions file
            df_valdef_persistent.to_csv("Exports/Definitions/ValueDefinitions.csv", index_label="ID")

        except:

            ## Always lock in case of code failure
            os.chmod(persistent_file_path, S_IREAD|S_IRGRP|S_IROTH)
    else:
        print("No new rows added")

        # Extra lock, just in case
        os.chmod(persistent_file_path, S_IREAD|S_IRGRP|S_IROTH)

    if return_updated_df:
        return df_valdef_new

# def valuedef_setinvalid(invalid_id_list, return_updated_df=False):
#     persistent_file_path = "Resources/__ReadOnly/__ValueDefinitions.csv"

#     # Pull the current dataframe
#     df_valdef_persistent = pd.read_csv(persistent_file_path, index_col="ID")\
#         .astype({"CUI":"string", "value":"string", "valid":"bool", "creation_date":"string", "invalid_date":"string"})

#     assert len(invalid_id_list) == len(set(invalid_id_list))
#     assert all(isinstance(x, int) for x in invalid_id_list)

#     # Update the values
#     df_valdef_persistent.loc[invalid_id_list, "valid"] = False
#     df_valdef_persistent.loc[invalid_id_list, "invalid_date"] = str(datetime.now().date())

#     df_valdef_persistent = df_valdef_persistent.astype({"CUI":"string", "value":"string", "valid":"bool", "creation_date":"string", "invalid_date":"string"})

#     if len(invalid_id_list) > 0:
#         try:
#             os.chmod(persistent_file_path, S_IWUSR|S_IREAD)
#             df_valdef_persistent.to_csv(persistent_file_path, index_label="ID")
#             print("UPDATED \"%s\"" % persistent_file_path)
#             os.chmod(persistent_file_path, S_IREAD|S_IRGRP|S_IROTH)
#             print("Set %d records as invalid" % len(invalid_id_list))
#         except:

#             ## Always lock in case of code failure
#             print("Writing to file FAILED")
#             os.chmod(persistent_file_path, S_IREAD|S_IRGRP|S_IROTH)
#             # Update the usable definitions file
#             df_valdef_persistent.to_csv("Exports/Definitions/ValueDefinitions.csv", index_label="ID")
#     else:
#         print("No new rows added")

#         # Extra lock, just in case
#         os.chmod(persistent_file_path, S_IREAD|S_IRGRP|S_IROTH)

#     if return_updated_df:
#         return df_valdef_persistent