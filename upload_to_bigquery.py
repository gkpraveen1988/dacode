import os
import pandas as pd
import numpy as np
import MySQLdb
#from google.oauth2 import service_account
from gcpauth import set_gcp_cred
import pandas_gbq
import hashlib


def mask_str(input_str):
    """Salts, hashes the given string using two random strings."""
    global random_str1, random_str2
    new_str = random_str1 + input_str.strip().lower() + random_str2
    return hashlib.sha256(new_str.encode('utf-8')).hexdigest().lower()


def get_credentials(fname):
    """Gets the credentials for the mysql db."""
    global basedir
    with open(os.path.join(basedir, fname), 'r') as f:
        return list(map(str.strip, f.readlines()))


def read_data(conn, table_name, query=None):
    """Reads the given table from the mysql db."""
    print(f'Querying {table_name}...')
    if query is None:
        return pd.read_sql(f'select * from {table_name}', conn)
    else:
        return pd.read_sql(query, conn)


def upload_data(data, destination_table, if_exists='replace'):
    """Uploads the data to the BigQuery."""
    print(f'Uploading {destination_table} data to BigQuery...')
    pandas_gbq.to_gbq(data, project_id='dotted-marking-256715', destination_table=destination_table,
                      if_exists=if_exists)


def mask_fields(data, masked_fields):
    """Masks the given fields/columns in the pandas dataframe..."""
    for field in masked_fields:
        # keep the null values...
        data[field] = data[field].apply(
            lambda s: mask_str(str(s)) if s and s != np.nan else None)
    return data


def upload_table(conn, table_name, query=None, if_exists='replace', masked_fields=None,
                 collection='da_portal_masked'):
    """Queries and uploads the whole table to BigQuery."""
    data = read_data(conn, table_name, query=query)
    if masked_fields is not None:
        data = mask_fields(data, masked_fields)
    destination_table = f'{collection}.{table_name}'
    upload_data(data, destination_table, if_exists)


def upload_audit(conn):
    """Uploads the audit table - note that this procedure only uploads the whole audit table if
    there was no upload before."""
    global basedir
    # get the previous max audit id
    try:
        f = open('da_audit_max_id', 'r')
        max_id = int(f.readline())
        f.close()
        if_exists = 'append'
    except:
        max_id = None
        if_exists = 'replace'
    if max_id is None:
        audit_sql = """select audit_id, user_id, method, status, insert_date_time from da_audit
                       union select
                       audit_id, user_id, method, status, insert_date_time from da_audit_bkp"""
    else:
        audit_sql = f"""select audit_id, user_id, method, status, insert_date_time
                        from da_audit
                        where audit_id > {max_id}"""
    audit = read_data(conn, 'da_audit', query=audit_sql)
    if len(audit) > 0:
        # update the max audit id...
        max_id = audit['audit_id'].max()
        with open(os.path.join(basedir, 'da_audit_max_id'), 'w') as f:
            f.write(f'{max_id}\n')
        upload_data(audit, 'consults.da_audit', if_exists=if_exists)


def upload_pat_prescription_list(conn):
    query = """
    select prescription_list_id,
           patient_id,
           version_num,
           current_version_flag,
           consult_id,
           created_by_user_id,
           created_by_clinic_id,
           created_by_pharmacy_id,
           created_date_time,
           replace(replace(notes_to_patient, char(13), ''), char(10), '') as notes_to_patient,
           replace(replace(remarks, char(13), ''), char(10), '') as remarks,
           allergies,
           trim(diagnosis_desc) as diagnosis_desc,
           diagnosis_code,
           status,
           show_cashless,
           reason_for_change,
           update_date_time,
           insert_date_time
    from da_pat_prescription_list
    """
    upload_table(conn, 'da_pat_prescription_list', query=query)


def upload_pat_medicine_items(conn):
    query = """
    select pat_medicine_item_id,
           prescription_list_id,
           prescription_list_version_num,
           seq_num,
           medicine_id,
           medicine_local_desc,
           medicine_local_code,
           medicine_standard_desc,
           medicine_standard_code,
           quantity,
           quantity_type_desc,
           quantity_type_code,
           dosage,
           dosage_unit_desc,
           dosage_unit_code,
           frequency_desc,
           frequency_code,
           best_time_desc,
           best_time_code,
           replace(replace(remarks, char(13), ''), char(10), '') as remarks,
           insert_date_time
    from da_pat_medicine_items
    """
    upload_table(conn, 'da_pat_medicine_items', query=query)


def upload_Contact_(conn):
    query = """
    select userId, companyId, userName, createDate, modifiedDate, gender, birthday, age,
           emailAddress, firstName, lastName,
           concat(lower(trim((case when firstName is null then '' else firstName end ))), ' ',
                  lower(trim((case when lastName is null then '' else lastName end )))) as name,
           concat(lower(trim((case when lastName is null then '' else lastName end ))), ' ',
                  lower(trim((case when firstName is null then '' else firstName end )))) as rname
    from Contact_
    """
    data = read_data(conn, 'Contact_', query=query)
    # replace null values with ''
    data = data.replace(np.nan, '', regex=True)
    data = mask_fields(data, ['emailAddress', 'name', 'rname', 'firstName', 'lastName'])
    upload_data(data, 'consults.Contact_')


def upload_plato(conn):
    for table in {t[1][0] for t in pd.read_sql('show tables;', conn).iterrows()}:
        if table == 'patient':
            data = read_data(conn, 'patient')
            if 'maskedNRIC' in set(data.columns):
                data = data.rename(columns={'maskedNRIC': 'nric'})
            data = mask_fields(data, ['nric', 'email', 'name'])
            upload_data(data, 'plato.patient')
        elif table == 'email':
            data = read_data(conn, 'email')
            data = mask_fields(data, ['email', 'html'])
            upload_data(data, 'plato.email')
        else:
            upload_table(conn, table, collection='plato')


def upload_insurance(conn):
    upload_table(conn, 'group_insurance_user',
                 masked_fields=['email', 'dob', 'name', 'identification'], collection='insurance')
    upload_table(conn, 'insurance_user_mapping',
                 masked_fields=['dob', 'full_name', 'identification_num'], collection='insurance')


if __name__ == '__main__':
    basedir = os.path.dirname(os.path.realpath(__file__))
    host, db, user, password = get_credentials(os.path.join(basedir, 'mysql_credentials.txt'))
    #phost, pdb, puser, ppassword = get_credentials(os.path.join(basedir, 'plato_credentials.txt'))
    #whost, wdb, wuser, wpassword = get_credentials(
    #    os.path.join(basedir, 'insurance_credentials.txt'))
    da_portal = MySQLdb.connect(host=host, user=user, passwd=password, db=db)
    set_gcp_cred()
    #service_account.Credentials.from_service_account_file(
    #    os.path.join(basedir, 'da-datascience-f59efde7f810.json'))

    # read the random strings from
    with open(os.path.join(basedir, 'random_strings.txt'), 'r') as f:
        random_str1, random_str2 = list(map(str.strip, f.readlines()))

    # custom functions for uploading tables...
    #upload_pat_prescription_list(da_portal)
    #upload_pat_medicine_items(da_portal)
    #upload_audit(da_portal)

    tables = [
        'da_consult', 'da_payment_log', 'da_group_users', 'da_doctor_details',
        'da_consult_docs', 'da_group', 'da_purchase_list',
        'da_purchase_list_items', 'da_referral', 'da_consult_ext', 'Company', 'ExpandoColumn',
        'ExpandoValue', 'da_marketplace_cat_item_map', 'ClassName_', 'da_health_screening_package',
        'da_promo'
    ]

    upload_table(da_portal, 'da_vendor', masked_fields=['email'])
    upload_table(da_portal, 'da_health_screening_audit',
                 masked_fields=['patient_email', 'patient_name'])

    '''
    for table_name in tables:
        upload_table(da_portal, table_name)
    '''

    upload_table(da_portal, 'User_', masked_fields=['emailAddress'])
    upload_Contact_(da_portal)
    upload_table(da_portal, 'MaskedNRIC', masked_fields=['maskedNRIC'])
    
    '''
    insurance = MySQLdb.connect(host=whost, user=wuser, passwd=wpassword, db=wdb)
    upload_insurance(insurance)

    plato = MySQLdb.connect(host=phost, user=puser, passwd=ppassword, db=pdb)
    upload_plato(plato)
    '''
