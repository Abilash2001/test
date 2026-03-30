import boto3
from boto3 import Session
from botocore.session import get_session
from botocore.credentials import RefreshableCredentials
import logging 
import sys

from typing import Dict


logger = logging.getLogger(__file__)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

console_handler = logging.StreamHandler(sys.stdout)
console_error_handler = logging.StreamHandler(sys.stderr)
console_handler.setFormatter(formatter)
console_error_handler.setFormatter(formatter)

console_handler.setLevel(logging.INFO)
console_error_handler.setLevel(logging.INFO)

if not logger.handlers:
    logger.addHandler(console_handler)
    logger.addHandler(console_error_handler)

logger.setLevel(logging.INFO)




# worker iam role has to be passed for sts assume
AWS_ROLE_ARN = "arn:aws:iam::713881786078:role/didaw-tsel-migration-lambda-role"
MAX_RETRY=3

def refresh_tokens() -> Dict[str,str] | None:
    global logger
    """
      Callback function that aws boto3 will use when the token is 
      about to expire
    """
    logger.info("Refreshing the creds...")
    sts_client = boto3.client('sts')
    try:
        global AWS_ROLE_ARN

        resp = sts_client.assume_role(
            RoleArn = AWS_ROLE_ARN,
            RoleSessionName = 'sqs-worker-auto-refresh',
            DurationSeconds =900
        )

        creds = resp["Credentitals"]
        return {
            "access_key": creds["AccessKeyId"],
            "secret_key": creds["SecretAccessKey"],
            "token": creds["SessionToken"],
            "expiry_time": creds["Expiration"].isoformat()
        }
    except Exception as e:
        global MAX_RETRY
        if MAX_RETRY ==0:            
            logger.exception("Failed to fetch refreshed token, (refresh)",exc_info=True)
        else:
            MAX_RETRY = MAX_RETRY-1
            logger.info("Failed to refresh (reason) => %s\n",e)
            logger.info(" %s Retrying %s", "*"*5,"*"*5)
            logger.info("\n%s","*"*10)
            return refresh_tokens()



def get_aws_session() -> Session:
    """
        To create new refreshable session, we can use this session for each
        worker individually.

        NOTE: Aws sesssion are not process safe so we should use one session per worker
    """
    refreshable_creds = RefreshableCredentials.create_from_metadata(
                                    metadata=refresh_tokens(),
                                    refresh_using=refresh_tokens,
                                    method="sts-assume-role"
                        )
    boto_session = get_session()
    boto_session._credentials = refreshable_creds
    new_session =Session(botocore_session=boto_session)
    return new_session
    