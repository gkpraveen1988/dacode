import boto3
import os
def get_secret():
    secret_name = ""
    region_name = "ap-southeast-1"
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
        return get_secret_value_response['SecretString']
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            raise e
    else:
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])

def set_gcp_cred():
    get_secret_value_response = get_secret()
    current_path = os.getcwd()
    flname = ''
    fl = open(flname,"w")
    fl.write(get_secret_value_response)
    fl.close()
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = flname
    return "ENV Set!"
    #print os.environ['GOOGLE_APPLICATION_CREDENTIALS']
