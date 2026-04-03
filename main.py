import sys
from typing import List,Tuple
import os
import gc

# 3rdPartyLib
from dotenv import load_dotenv,find_dotenv

# User Defined
import tokens
import logging_consumer as log
import publisher as pub



def consume_message(main_logger: log.loggy.logging.Logger,session,kind: str,rabbit_conn: pub.RqPublisher):
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
            if len(result)!=0:
                success_ids = rabbit_conn.publish_message(result)
                rabbit_conn.publish_returned_messaged_to_sqs_dlq()                
                if len(success_ids)!=0:
                    for id in success_ids:
                        receipt_id = result[id]["ReceiptHandle"]
                        sqs_client.delete_message(
                            QueueUrl = queue_name,
                            ReceiptHandle = receipt_id
                        )
            break
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
    main_logger = log.get_logger(__file__)
    main_logger.info(f"Got env: {get_env}")

    if get_env is not None and get_kind is not None:
        load_current_env(get_env)
        session = tokens.get_aws_session()        
        sqs_pub_obj = pub.SqsPublisher(main_logger=main_logger,kind=get_kind)        
        rabbit_pub_obj = pub.RqPublisher(main_logger=main_logger,sqs_pub=sqs_pub_obj,kind=get_kind)
        consume_message(main_logger,session,get_kind,rabbit_pub_obj)
    else:
        main_logger.error("Input variable are not passed")
    
    