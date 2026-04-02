import tokens
import os
import boto3

class SqsPublisher:
    def __init__(self,main_logger,kind) -> None:
        self.session = tokens.get_aws_session()
        self.main_logger = main_logger
        self.queue_name = os.environ.get(f"{kind}_SQS_DLQ_QUEUE_URL") or ""
        self.kind = kind
        self.conn = self.session.client("sqs")        
    

    def publish_message(self,message):
        self.main_logger.info(f"Sending DLQ message for {self.kind}")
        resp = self.conn.send_message(MessageBody=message)
        self.main_logger.info(resp["MessageId"])
    