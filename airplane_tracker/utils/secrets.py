import boto3, json
from botocore.exceptions import ClientError

def get_secret(name, region="eu-north-1"):
    client = boto3.client("secretsmanager", region_name=region)
    try:
        resp = client.get_secret_value(SecretId=name)
    except ClientError as e:
        raise
    return json.loads(resp["SecretString"])