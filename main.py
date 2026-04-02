import sys
from datetime import datetime
from typing import List,Optional,Literal,Tuple
import os
import gc
# 3rdPartyLib
from dotenv import load_dotenv,find_dotenv
import boto3

# User Defined
import tokens
import sqs_logger



def consume_message(main_logger: sqs_logger.loggy.logging.Logger,session,kind: str):
    sqs_client = session.client("sqs")
    queue_name = os.environ.get(f"{kind}_SQS_QUEUE_URL")
    print("\n"*3)
    while True:
        try:
            response = sqs_client.receive_message(
                QueueUrl=queue_name,
                WaitTimeSeconds= int(os.environ.get("WAIT_TIME_IN_SECONDS","20")),
                MaxNumberOfMessages=int(os.environ.get("MAXIMUM_NO_OF_BATCH_MESSAGES","10")),
                VisibilityTimeout=int(os.environ.get("VISIBILITY_TIMEOUT","60"))
            )
            result= response.get("Messages",[])
            main_logger.info(f"GOT: {len(result)} messages\r")
        except Exception as e:
            gc.collect()
            main_logger.exception(f"Failed to poll the {kind} SQS, (reason) =>{str(e)}\n",exc_info=True)



def parse_argument() -> Tuple[str|None,str|None]:
    args: List[str] = sys.argv
    env_result,queue_type=None,None
    if "--env" in args:
        if args.index("--env")+1 >= len(args):
            raise ValueError("Need atleast one env value from this list [dev,qa,prod]")
        env_result=  args[args.index("--env")+1]
        if env_result not in ["dev","qa","prod"]:
            raise ValueError("Need atleast one env value from this list [dev,qa,prod]")
    
    if "--kind" in args:
        if args.index("--kind")+1 >= len(args):
            raise ValueError("Need atleast one kind value from this list [NODE,DASS,PPOD]")
        queue_type=  args[args.index("--kind")+1]
        if queue_type not in ["NODE","DASS","PPOD"]:
            raise ValueError("Need atleast one kind value from this list [NODE,DASS,PPOD]")
    
    if env_result is None or queue_type is None:
        raise ValueError("Both --env and --kind should be passed to the argument")
    else:
        return (env_result,queue_type)

def load_current_env(get_env: str) -> None:
    path: str = f"./config/.env.{get_env}"
    if not load_dotenv(find_dotenv(path),override=True):
        raise FileNotFoundError(f"{path} no such config exist")    

if __name__ == "__main__":
    get_env,get_kind = parse_argument()
    main_logger = sqs_logger.get_logger(__file__)
    main_logger.info(f"Got env: {get_env}")

    if get_env is not None and get_kind is not None:
        load_current_env(get_env)
        session = tokens.get_aws_session()
        consume_message(main_logger,session,get_kind)
    
    