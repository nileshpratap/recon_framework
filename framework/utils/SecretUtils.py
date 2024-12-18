import logging as logs
import boto3
import json
from botocore.exceptions import ClientError
from framework.utils.ConfigUtils import ConfigUtils


class SecretUtils():
    @staticmethod
    def getSecret(secret_name, region_name="ap-south-1"):
        logs.basicConfig()
        logs.getLogger().setLevel(logs.INFO)
        # session = boto3.session.Session()
        client = boto3.client(service_name='secretsmanager', region_name=region_name)
        logs.info("::::Getting Secret of {}".format(secret_name))
        try:
            get_secret_value_response = client.get_secret_value(SecretId=secret_name)
            secret = json.loads(get_secret_value_response['SecretString'])

            return secret
        except ClientError as e:
            if e.response['Error']['Code'] == 'DecryptionFailureException':
                # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'InternalServiceErrorException':
                # An error occurred on the server side.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'InvalidParameterException':
                # You provided an invalid value for a parameter.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'InvalidRequestException':
                # You provided a parameter value that is not valid for the current state of the resource.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'ResourceNotFoundException':
                # We can't find the resource that you asked for.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            return ""
