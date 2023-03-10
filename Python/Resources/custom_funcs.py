from cmath import exp
from numpy import int64
import os
import json
import base64
import pickle
import re
import sqlite3
from getpass import getpass
import pandas as pd
from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from datamanagement import get_eldef, get_valdef, get_origindex
import datetime

def get_vocab_ids(vocab=["SNOMED"], cols=["concept_code", "concept_name", "vocabulary_id", "concept_id"], path_to_CONCEPT="Vocabularies/CONCEPT.csv"):
    """
    Get subset of records in the OMOP concept table

    Arguments:
        
        vocab: list, default ["SNOMED"]
            The list of vocabularies you want to incldue

        cols: list, default ["concept_code", "concept_name", "vocabulary_id", "concept_id"]
            The list of columns you want from the concept table

        path_to_CONCEPT: str, default "Vocabularies/CONCEPT.csv"
            Path to the OMOP CONCEPT csv table

    Returns:
        df_concept: pandas.Dataframe
            pandas DataFrame containing the records requested
    """
    dtype_map = {"concept_code": object, "concept_name": "string", "vocabulary_id":"string", "concept_id":"int64"}
    df_all_concepts = pd.read_csv(path_to_CONCEPT, delimiter="\t", usecols=cols, dtype=dtype_map)
    df_concept = df_all_concepts.loc[df_all_concepts.vocabulary_id.isin(vocab)]
    del df_all_concepts
    return df_concept

def load_encrypted_dataframe(path, password):
    """
    Load a pandas DataFrame from one encrypted using store_encrypted_dataframe()

    Arguments:
        path: str
            Path to the encrypted DataFrame

        password: str
            Password to the encrypted DataFrame

    Returns:
        data: pd.DataFrame
            Un-encrypted pandas DataFrame
    """
    # Create a password
    pwd_bytes = bytes(password, 'utf-8')

    # Get the salt
    with open("TestData/PatData/salt.txt", 'r') as f:
        line = f.readline()
        m_salt = bytes.fromhex(line)

    # Hash the salt and password to get the key
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=m_salt,
        iterations=390000,
    )
    key = base64.urlsafe_b64encode(kdf.derive(pwd_bytes))

    # Create the fernet object
    fernet = Fernet(key)

    # Load the encrypted file
    with open(path, 'rb') as encrypted_file:
        encrypted = encrypted_file.read()
 
    # Decrypt the file
    try:
        return pickle.loads(fernet.decrypt(encrypted))
    except InvalidToken:
        raise(ValueError("Incorrect password"))
    

def store_encrypted_dataframe(df, path, password):
    """
    Takes a pandas DataFrame and stores it in an encrypted pickle

    Arguments:
        df: pd.DataFrame
            The pandas DataFrame to be stored

        path: str
            Path to where the DataFrame should be stored

        password: str
            Password to the encrypted DataFrame
        
    Returns:
        data: pd.DataFrame
            Un-encrypted pandas DataFrame
    """
    # Create a password
    pwd_bytes = bytes(password, 'utf-8')
    # Get the salt
    with open("TestData/PatData/salt.txt", 'r') as f:
        salt = f.readline()
    # Hash the salt and password to get the 
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=bytes.fromhex(salt),
        iterations=390000,
    )
    key = base64.urlsafe_b64encode(kdf.derive(pwd_bytes))
    fernet = Fernet(key)

    my_pickle = pickle.dumps(df)
    encrypted = fernet.encrypt(my_pickle)
    with open(path, "wb") as encrypted_file:
        encrypted_file.write(encrypted)

def combine_exam_element_columns(df: pd.DataFrame, combine_column_name="NAMEMATCH", examareacol=None, dataelementcol=None):
    """
    Creates the unique EPIC string name used for joining EPIC source elements with different CUIS
    
    Arguments:
        df: pd.DataFrame
            DataFrame with a column for exam area and a column for data element

        combine_column_name: string, default "NAMEMATCH"
            Name for the new column

        examareacol: string, default None
            If not provided, exam area column is automatically detected

        dataelementcol: string, default None
            If not provided, data element column is automatically detected        

    Returns:
        combined_df: pd.DataFrame
            DataFrame with an additional row called NAMEMATCH (or combine_column_name if specified), that's a mashup of the exam area and data element columns
    """
    
    if examareacol is None:
        possible_names = ["Exam Area", "examArea", "ADD_INFO:Exam Area"]
        for name in possible_names:
            if name in df.columns:
                examareacol = name
                break
    
    if dataelementcol is None:
        possible_names = ["Data Element", "dataElement", "ADD_INFO:Data Element"]
        for name in possible_names:
            if name in df.columns:
                dataelementcol = name
                break
    
    assert(examareacol is not None)
    assert(dataelementcol is not None)

    combined_df = df.copy(deep=True)

    combined_df.loc[:,combine_column_name] = df[examareacol] + "-" + df[dataelementcol]

    return combined_df

def combine_NAMEMATCH_value_columns(df: pd.DataFrame, combine_column_name="VALSTRKEY"):
    """
    Creates the unique EPIC string name used for joining EPIC source elements with different CUIS
    
    Arguments:
        df: pd.DataFrame
            DataFrame with a column called "NAMEMATCH" and a column called "value"

        combine_column_name: string
            Name for the new column

    Returns:
        combined_df: pd.DataFrame
            DataFrame with an additional row called VALSTRKEY (or combine_column_name if specified), that's a mashup of the exam area and data element columns
    """
    combined_df = df.copy(deep=True)

    combined_df.loc[:,combine_column_name] = df["NAMEMATCH"] + "-" + df["value"]

    return combined_df

def set_compare(el_set1, el_set2):
    """Lists some basic information about two element sets"""
    print("Size of set 1: %d" % len(el_set1))
    print("Size of set 2: %d" % len(el_set2))
    print("Intersect size: %d" % len(el_set1 & el_set2))
    print("Set 1 NOT set 2: %d" % len(el_set1 - el_set2))
    print("Set 2 NOT set 1: %d" % len(el_set2 - el_set1))

def verify_disjoint(set_list):
    """Takes a list of sets and verifies that they're disjoint"""
    union_length = len(set.union(*set_list))
    sum_length = 0
    for m_set in set_list:
        sum_length += len(m_set)
        
    return sum_length == union_length

def get_list_from_column(cur, table, column):
    cur.row_factory = lambda cursor, row: row[0]
    sql_query = "SELECT %s FROM %s""" % (column, table)
    m_list = cur.execute(sql_query).fetchall()
    cur.row_factory = None
    return m_list

def has_laterality(vals: pd.Series, side="right"):
    right_reg_string = r"(?i)\bright\b"
    left_reg_string = r"(?i)\bleft\b"
    
    assert (side=="left") or (side=="right")
    if side=="right":
        assert (~vals.str.contains(left_reg_string)).all()
        return vals.str.contains(right_reg_string)
    if side=="left":
        assert (~vals.str.contains(right_reg_string)).all()
        return vals.str.contains(left_reg_string)

def filter_for_laterality_terms(vals: pd.Series, side="right"):
    right_reg_string = r"(?i)\bright\b"
    left_reg_string = r"(?i)\bleft\b"

    assert (side=="left") or (side=="right")
    if side=="right":
        return vals.str.contains(right_reg_string)
    if side=="left":
        return vals.str.contains(left_reg_string)

def expand_flags(df_in, exclusion_terms=["LOINC"]):

    df_analyse = df_in.copy(deep=True)

    flag_list = []
    for index, row in df_analyse.iterrows():
        ## Check that the equivalence rows are valid
        assert row.equivalence in ["EQUAL", "WIDER", "NARROWER", "UNMATCHED"]

        if type(row.comment) == str:
            row_flags = re.findall(r"\b[A-Z]{5,}\b",row.comment)

            # Remove any named things to exclude
            for term in exclusion_terms:
                try:
                    row_flags.remove(term)
                except ValueError:
                    pass

            
            for val in row_flags:
                if val not in flag_list:
                    flag_list.append(val)
                df_analyse.loc[index, val] = 1
    df_analyse.loc[:, flag_list] = df_analyse.loc[:, flag_list].fillna(0).astype("int")

    return df_analyse

def analyze_mapping(df_in, exclusion_terms=["LOINC"], get_dict=True, print_vals=False, analysis_version=1):

    df_analyse = df_in.copy(deep=True)
    dict_out = {}

    flag_list = []
    for index, row in df_analyse.iterrows():
        ## Check that the equivalence rows are valid
        assert row.equivalence in ["EQUAL", "WIDER", "NARROWER", "UNMATCHED"]

        if type(row.comment) == str:
            row_flags = re.findall(r"\b[A-Z]{5,}\b",row.comment)

            # Remove any named things to exclude
            for term in exclusion_terms:
                try:
                    row_flags.remove(term)
                except ValueError:
                    pass

            
            for val in row_flags:
                if val not in flag_list:
                    flag_list.append(val)
                df_analyse.loc[index, val] = 1

    if print_vals: print("---COUNTS FOR EQUIVALENCE---")
    dict_equiv = {}

    df_equivcounts = df_analyse.equivalence.value_counts()
    for equiv in ["EQUAL", "WIDER", "NARROWER", "UNMATCHED"]:
        if print_vals: print(equiv, df_equivcounts.loc[equiv])
        dict_equiv[equiv] = int(df_equivcounts.loc[equiv])

    dict_out["equivalence"] = dict_equiv

    if print_vals: print("")

    if print_vals: print("---Flag counts for UNMAPPED---")
    dict_unmapped = {}

    # Switch based on update to analysis requested by CXC.
    assert (analysis_version == 1) or (analysis_version == 2)

    if analysis_version == 1:
        try:
            assert (df_analyse.loc[df_analyse.VALSMAPPED.notnull()].equivalence == "UNMATCHED").all()
            count_VALSMAPPED = df_analyse.loc[df_analyse.VALSMAPPED.notnull()].shape[0]
            if print_vals: print("VALSMAPPED: %d" % count_VALSMAPPED)
            dict_unmapped["VALSMAPPED"] = int(count_VALSMAPPED)
        except AttributeError:
            pass

        assert (df_analyse.loc[df_analyse.NOMATCH.notnull()].equivalence == "UNMATCHED").all()
        count_NOMATCH = df_analyse.loc[df_analyse.NOMATCH.notnull()].shape[0]
        if print_vals: print("NOMATCH: %d" % count_NOMATCH)
        dict_unmapped["NOMATCH"] = int(count_NOMATCH)

        try:
            assert (df_analyse.loc[df_analyse.INDIRECT.notnull()].equivalence == "UNMATCHED").all()
            count_INDIRECT = df_analyse.loc[df_analyse.INDIRECT.notnull()].shape[0]
            if print_vals: print("INDIRECT: %d" % count_INDIRECT)
            dict_unmapped["INDIRECT"] = int(count_INDIRECT)
        except AttributeError:
            pass

        assert (df_analyse.loc[df_analyse.SUBFIELD.notnull()].equivalence == "UNMATCHED").all()
        count_SUBFIELD = df_analyse.loc[df_analyse.SUBFIELD.notnull()].shape[0]
        if print_vals: print("SUBFIELD: %d" % count_SUBFIELD)
        dict_unmapped["SUBFIELD"] = int(count_SUBFIELD)

        dict_out["unmapped"] = dict_unmapped

    if analysis_version == 2:

        ## Collect the counts for 'OTHER'
        dict_unmapped["OTHER"] = 0

        try:
            assert (df_analyse.loc[df_analyse.VALSMAPPED.notnull()].equivalence == "UNMATCHED").all()
            count_VALSMAPPED = df_analyse.loc[df_analyse.VALSMAPPED.notnull()].shape[0]
            if print_vals: print("VALSMAPPED: %d" % count_VALSMAPPED)
            dict_unmapped["OTHER"] += int(count_VALSMAPPED)
        except AttributeError:
            pass
        try:
            assert (df_analyse.loc[df_analyse.INDIRECT.notnull()].equivalence == "UNMATCHED").all()
            count_INDIRECT = df_analyse.loc[df_analyse.INDIRECT.notnull()].shape[0]
            if print_vals: print("INDIRECT: %d" % count_INDIRECT)
            dict_unmapped["OTHER"] += int(count_INDIRECT)
        except AttributeError:
            pass
        try:
            assert (df_analyse.loc[df_analyse.SUBFIELD.notnull()].equivalence == "UNMATCHED").all()
            count_SUBFIELD = df_analyse.loc[df_analyse.SUBFIELD.notnull()].shape[0]
            if print_vals: print("SUBFIELD: %d" % count_SUBFIELD)
            dict_unmapped["OTHER"] += int(count_SUBFIELD)
        except AttributeError:
            pass

        isother_element_filter = append_sourceel_names(df_analyse).dataElement.isin(["Comments", "Users"])

        df_analyse.loc[isother_element_filter].shape[0]

        # Handle the NOMATCH, splitting out the COMMENTS and USERS
        assert (df_analyse.loc[df_analyse.NOMATCH.notnull()].equivalence == "UNMATCHED").all()
        count_NOMATCH_ISOTHER_FILTERED = df_analyse.loc[df_analyse.NOMATCH.notnull() & isother_element_filter].shape[0]
        count_NOMATCH = df_analyse.loc[df_analyse.NOMATCH.notnull() & ~isother_element_filter].shape[0]
        assert df_analyse.loc[df_analyse.NOMATCH.notnull()].shape[0] == (count_NOMATCH_ISOTHER_FILTERED + count_NOMATCH)
        if print_vals: print("NOMATCH: %d" % count_NOMATCH)
        dict_unmapped["OTHER"] += int(count_NOMATCH_ISOTHER_FILTERED)
        dict_unmapped["NOMATCH"] = int(count_NOMATCH)

        dict_out["unmapped"] = dict_unmapped

    if print_vals: print("")

    if print_vals: print("---Flag counts for WIDER---")

    dict_wider = {}

    assert (df_analyse.loc[df_analyse.LATERALITY.notnull()].equivalence == "WIDER").all()
    count_LATERALITY = df_analyse.loc[df_analyse.LATERALITY.notnull() & df_analyse.CONCEPTMISSING.isnull()].shape[0]
    if print_vals: print("LATERALITY: %d" % count_LATERALITY)
    dict_wider["LATERALITY"] = int(count_LATERALITY)

    assert (df_analyse.loc[df_analyse.CONCEPTMISSING.notnull()].equivalence == "WIDER").all()
    count_CONCEPTMISSING = df_analyse.loc[df_analyse.CONCEPTMISSING.notnull() & df_analyse.LATERALITY.isnull()].shape[0]
    if print_vals: print("CONCEPTMISSING: %d" % count_CONCEPTMISSING)
    dict_wider["CONCEPTMISSING"] = int(count_CONCEPTMISSING)

    count_CONCEPTMISSINGandLATERALITY = df_analyse.loc[df_analyse.CONCEPTMISSING.notnull() & df_analyse.LATERALITY.notnull()].shape[0]
    if print_vals: print("CONCEPTMISSING&LATERALITY: %d" % count_CONCEPTMISSINGandLATERALITY)
    dict_wider["CONCEPTMISSING&LATERALITY"] = int(count_CONCEPTMISSINGandLATERALITY)

    dict_out["wider"] = dict_wider

    if get_dict:
        return dict_out

def rows_by_equiv_and_flag(df_in, flag_term, equiv_term):
    df_analyse = df_in.copy(deep=True)

    for index, row in df_analyse.iterrows():
        ## Check that the equivalence rows are valid
        assert row.equivalence in ["EQUAL", "WIDER", "NARROWER", "UNMATCHED"]

        if type(row.comment) == str:
            row_flags = re.findall(r"\b[A-Z]{5,}\b",row.comment)
            
            if flag_term in row_flags:
                df_analyse.loc[index, flag_term] = True
            else:
                df_analyse.loc[index, flag_term] = False
        else:
            df_analyse.loc[index, flag_term] = False
        
    return df_analyse.loc[df_analyse[flag_term] & (df_analyse.equivalence == equiv_term)]

def append_concept_names(df_in: pd.DataFrame, conceptid_colname="conceptId", resource_db_path=r"Resources\resource.db"):
    sqliteConnection = sqlite3.connect(resource_db_path)
    cursor = sqliteConnection.cursor()

    df = df_in.copy(deep=True)

    # Rename conceptId columns and drop null values
    df.rename(columns={conceptid_colname:"conceptId"}, inplace=True)

    if (df.conceptId.isna().any()):
        raise Exception("Null value found in ConceptID field")

    # Write this to a temporary table
    df.to_sql(name="concept_id_temp_table", con=sqliteConnection, if_exists="replace", index=False)

    # Extract the names for these conceptId values
    m_query = """
    WITH valid_list AS (
        SELECT conceptId
        FROM concept_id_temp_table
    ), concept_data AS (
        SELECT concept_id, concept_name
        FROM concept
        WHERE concept_id IN valid_list
    )
    SELECT DISTINCT valid_list.conceptId, concept_data.concept_name
    FROM valid_list
    LEFT JOIN concept_data ON valid_list.conceptId=concept_data.concept_id
    """
    expanded_names = pd.read_sql(m_query, con=sqliteConnection).astype({"conceptId":"Int64"})

    # return expanded_names

    assert expanded_names.conceptId.is_unique
    
    cursor.execute("DROP TABLE concept_id_temp_table")
    sqliteConnection.close()

    return df.merge(expanded_names, on="conceptId", how="left")

def append_sourceconcept_id(df_in: pd.DataFrame, conceptid_colname="conceptId", resource_db_path=r"Resources\resource.db"):
    sqliteConnection = sqlite3.connect(resource_db_path)
    cursor = sqliteConnection.cursor()

    df = df_in.copy(deep=True)

    # Rename conceptId columns and drop null values
    df.rename(columns={conceptid_colname:"conceptId"}, inplace=True)

    if (df.conceptId.isna().any()):
        raise Exception("Null value found in conceptId field")

    # Write this to a temporary table
    df.to_sql(name="concept_id_temp_table", con=sqliteConnection, if_exists="replace", index=False)

    # Extract the names for these conceptId values
    m_query = """
    WITH valid_list AS (
        SELECT conceptId
        FROM concept_id_temp_table
    ), concept_data AS (
        SELECT concept_id, vocabulary_id, concept_code
        FROM concept
        WHERE concept_id IN valid_list
    )
    SELECT DISTINCT valid_list.conceptId, concept_data.concept_code
    FROM valid_list
    LEFT JOIN concept_data ON valid_list.conceptId=concept_data.concept_id
    """

    expanded_names = pd.read_sql(m_query, con=sqliteConnection).astype({"conceptId":"Int64"})

    # return expanded_names

    assert expanded_names.conceptId.is_unique
    
    cursor.execute("DROP TABLE concept_id_temp_table")
    sqliteConnection.close()

    return df.merge(expanded_names, on="conceptId", how="left")

def append_vocabulary_id(df_in: pd.DataFrame, conceptid_colname="conceptId", resource_db_path=r"Resources\resource.db"):
    sqliteConnection = sqlite3.connect(resource_db_path)
    cursor = sqliteConnection.cursor()

    df = df_in.copy(deep=True)

    # Rename conceptId columns and drop null values
    df.rename(columns={conceptid_colname:"conceptId"}, inplace=True)

    if (df.conceptId.isna().any()):
        raise Exception("Null value found in conceptId field")

    # Write this to a temporary table
    df.to_sql(name="concept_id_temp_table", con=sqliteConnection, if_exists="replace", index=False)

    # Extract the names for these conceptId values
    m_query = """
    WITH valid_list AS (
        SELECT conceptId
        FROM concept_id_temp_table
    ), concept_data AS (
        SELECT concept_id, vocabulary_id
        FROM concept
        WHERE concept_id IN valid_list
    )
    SELECT DISTINCT valid_list.conceptId, concept_data.vocabulary_id
    FROM valid_list
    LEFT JOIN concept_data ON valid_list.conceptId=concept_data.concept_id
    """

    expanded_names = pd.read_sql(m_query, con=sqliteConnection).astype({"conceptId":"Int64"})

    # return expanded_names

    assert expanded_names.conceptId.is_unique
    
    cursor.execute("DROP TABLE concept_id_temp_table")
    sqliteConnection.close()

    return df.merge(expanded_names, on="conceptId", how="left")

def append_sourceel_names(df_in: pd.DataFrame, sourcecode_colname="sourceCode", sourcecode_outcolname="sourceCode"):

    df = df_in.copy(deep=True)
    df.rename(columns={sourcecode_colname:sourcecode_outcolname}, inplace=True)

    df_eldef = get_eldef(); 
    assert df_eldef.CUI.is_unique
    df_eldef.rename(columns={"CUI":sourcecode_outcolname}, inplace=True)

    return df.merge(df_eldef, on=sourcecode_outcolname, how="left")

def append_sourceval_names(df_in: pd.DataFrame, sourcecode_colname="sourceCode"):

    df = df_in.copy(deep=True)
    df.rename(columns={sourcecode_colname:"sourceCode"}, inplace=True)

    df_valdef = get_valdef(); 
    assert df_valdef.ID.is_unique
    df_valdef.rename(columns={"ID":"sourceCode"}, inplace=True)

    return append_sourceel_names(df.merge(df_valdef, on="sourceCode", how="left"), sourcecode_colname="CUI", sourcecode_outcolname="CUI")

def append_sourceel_origindex(df_in: pd.DataFrame, sourcecode_colname="sourceCode", sourcecode_outcolname=None):
    if sourcecode_outcolname is None:
        sourcecode_outcolname = sourcecode_colname

    df = df_in.copy(deep=True)
    df.rename(columns={sourcecode_colname:sourcecode_outcolname}, inplace=True)
    
    df_origindex = get_origindex()
    assert df_origindex.CUI.is_unique
    df_origindex.rename(columns={"CUI":sourcecode_outcolname}, inplace=True)

    return df.merge(df_origindex, on=sourcecode_outcolname, how="left")

def extract_errors(path=r"C:\Users\willh\OneDrive - University of Cambridge\Work\University\Medicine\Elective\1 - OMOP workgroup collab\A - Vocab Mapping\SCREENEDMAPS\ErrorLog.txt"):
    with open(path, "r") as err_file:
        text = err_file.read().split("***FLAG ERRORS***")
        flag_text = text[1]
        oerr_text = text[0]
    
    sb_flag_keys = re.findall("(?<=SB.)(?<=\~)[\w|#]+", flag_text)
    cc_flag_keys = re.findall("(?<=CC.)(?<=\~)[\w|#]+", flag_text)
    sb_oerr_keys = re.findall("(?<=SB.)(?<=\~)[\w|#]+", oerr_text)
    cc_oerr_keys = re.findall("(?<=CC.)(?<=\~)[\w|#]+", oerr_text)   

    out_dict =  {
        "sb_flag_keys":sb_flag_keys,
        "cc_flag_keys":cc_flag_keys,
        "sb_oerr_keys":sb_oerr_keys,
        "cc_oerr_keys":cc_oerr_keys
    }

    with open("Exports/error_keys.txt", "w") as m_file:
        json.dump(out_dict, m_file)

def custom_filter(tuple_dfs: tuple, df_type=None):
    exclusion_examArea = ["Strabismus", "Contact Lens Current Rx", \
    "Contact Lens History", "Contact Lens Final Rx", "Contact Lens"]
    
    ret_tuple = ()
    if df_type not in ['element', 'value']:
            raise ValueError("Invalid/no dataframe type given: please specify \'element\' or \'value\'")
    for i, df in enumerate(tuple_dfs):
        match df_type:
            case 'element':
                expanded_df = append_sourceel_names(df)
            case 'value':
                expanded_df = append_sourceval_names(df)

        df_slice = df.loc[~expanded_df.examArea.isin(exclusion_examArea)]
        ret_tuple = (*ret_tuple, df_slice.copy(deep=True))
    return ret_tuple

def create_outdir():
    analysis_stamp = str(datetime.datetime.now().date())
    outdir = "Exports/" + analysis_stamp
    try:
        os.mkdir(outdir)
    except FileExistsError:
        pass

    try:
        os.mkdir(outdir + "/Analysis")
    except FileExistsError:
        pass

    try:
        os.mkdir(outdir + "/Analysis/FlagsExpanded")
    except FileExistsError:
        pass

    try:
        os.mkdir(outdir + "/MapCompare")
    except FileExistsError:
        pass
    
    try:
        os.mkdir(outdir + "/SSSOM")
    except FileExistsError:
        pass

    return outdir

def transform_mapping(dfmap_in, dftype=None):
    el_column_map = {
        "sourceCode": "subject_id",
        "SUBJECT_LABEL" : "subject_label",
        "equivalence" : "predicate_id",
        "conceptId" : "object_id",
        "concept_name" : "object_label",
        # : "mapping_justification",
        # : "mapping_date",
        # : "author_id",
        # : "subject_source",
        # : "subject_source_version",
        # : "object_source",
        # : "object_source_version",
        # : "confidence
    }

    predicate_map = {
        "EQUAL":"skos:exactMatch",
        "WIDER":"skos:broadMatch",
        "NARROWER":"skos:narrowMatch",
    }

    df_in = dfmap_in.copy(deep=True)

    if (dftype not in ["element", "value"]) or (dftype is None):
        raise Exception("No/invalid type (element vs value) specified")

    # Create the necessary columns
    if dftype == "element":
        # Create the necessary columns
        df_in = append_concept_names(combine_exam_element_columns(\
            append_sourceel_names(df_in), combine_column_name="SUBJECT_LABEL"))
    elif dftype == "value":
        df_in = append_concept_names(combine_NAMEMATCH_value_columns(\
            combine_exam_element_columns(append_sourceval_names(df_in)), combine_column_name="SUBJECT_LABEL"))
        df_in.loc[:,"sourceCode"] = df_in.loc[:,"sourceCode"].astype("string")
        

    #STEP 1: Rename columns
    df_in.rename(columns=el_column_map, inplace=True)

    #STEP 2: Convert values
    df_in.loc[:, "predicate_id"] = df_in.predicate_id.map(predicate_map)

    if dftype == "element":
        df_in.loc[:, "subject_id"] = "epic.kaleidoscope.common.CUI:" + df_in.subject_id
    elif dftype == "value":
        df_in.loc[:, "subject_id"] = "epic.kaleidoscope.common.prepopvalues:" + df_in.subject_id

    df_in.loc[:,"object_id"] = "ohdsi.concept:" + df_in.object_id.astype("string")

    df_in.loc[:,"mapping_justification"] = "semapv:HumanCuration"

    return df_in[\
        ['subject_id', 'subject_label', 'predicate_id', 'object_id', 'object_label', 'comment', 'mapping_justification']\
        ].loc[df_in.predicate_id.notna()]

def combine_analyse(eldict, valdict):
    outdict = {}
    for key1 in eldict:
        outdict[key1] = {}
        for key2 in eldict[key1]:
            if key2 in valdict[key1].keys():
                outdict[key1][key2] = eldict[key1][key2] + valdict[key1][key2]
            else:
                outdict[key1][key2] = eldict[key1][key2]
    return outdict

def verify_sourceCode_aligned(df1=None, df2=None):
    if (df1 is None) and (df2 is None):
        global df_el_sb, df_el_cc, df_val_sb, df_val_cc
        assert df_el_sb.sourceCode.equals(df_el_cc.sourceCode)
        assert df_val_sb.sourceCode.equals(df_val_cc.sourceCode)
    else:
        assert df1.sourceCode.equals(df2.sourceCode)