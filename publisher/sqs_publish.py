import os

import tokens

class SqsPublisher:
    def __init__(self,main_logger,kind) -> None:
        self.session = tokens.get_aws_session()
        self.main_logger = main_logger
        self.queue_name = os.environ.get(f"{kind}_SQS_DLQ_QUEUE_URL") or ""
        self.kind = kind
        self.conn = self.session.client("sqs")        
    

    def publish_message(self,messages):
        self.main_logger.info(f"Sending DLQ message for {self.kind}")
        failed_messsages= []
        if len(messages)==0:
            self.main_logger.info("Nothing to send DLQ")
        for message in messages:
            try:
                resp = self.conn.send_message(MessageBody=message.decode("utf-8"))
                self.main_logger.info(f"Sent successfully {resp["MessageId"]}")
            except Exception as e:
                failed_messsages.append(message)
                self.main_logger.exception(f"Failed to send DLQ {e}",exc_info=True)
        return failed_messsages
    