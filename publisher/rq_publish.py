
import pika
import os 

class RqPublisher:
   def __init__(self,main_logger,sqs_pub,kind) -> None:

      self.connection_parameter = {
         "host": os.environ.get("RABBIT_HOST") or "localhost",
         "port": os.environ.get("RABBIT_PORT") or "5672",
         "username": os.environ.get("RABBIT_USER") or "",
         "password": os.environ.get("RABBIT_PASS") or "",
         "vhost": os.environ.get("RABBIT_VHOST") or "/",
         "exchange": os.environ.get(f"{kind}_EXCHANGE_NAME") or "",
         "exchange_type": os.environ.get(f"{kind}_EXCHANGE_TYPE") or "direct",
         "queue_name": os.environ.get(f"{kind}_QUEUE_NAME") or "",
         "routing_key": os.environ.get(f"{kind}_ROUTING_KEY") or ""
      }      
      main_logger.info(f"Rabbitmq HOST = {self.connection_parameter['host']}")

      self.parameter= pika.ConnectionParameters(
         host=self.connection_parameter["host"],
         port=self.connection_parameter["port"],
         virtual_host=self.connection_parameter["vhost"],
         credentials=pika.PlainCredentials(username=self.connection_parameter["username"],password=self.connection_parameter["password"]),
         heartbeat=30,
         blocked_connection_timeout=300,
         socket_timeout=60
      )
      self.conn=None
      self.channel=None
      self.main_logger = main_logger
      self.returned_messages = []
      self.dlq_sqs = sqs_pub

   def publish_message_to_dlq_sqs(self,channel,method,property,body):
      try:
         self.dlq_sqs.publish_message(body.decode("utf-8"))
      except Exception:
         self.main_logger.exception(f"Failed to send message for {body.decode('utf-8')}\n",exc_info=True)

   def get_connection(self):
      self.main_logger.info("Trying to connect to rabbit mq")
      self.conn = pika.BlockingConnection(self.parameter)

      self.main_logger.info("Creating channel...")
      self.channel = self.conn.channel()
      
      self.main_logger.info("Enabling confirm delivery")
      self.channel.confirm_delivery()

      self.main_logger.info("Adding return callback")
      self.channel.add_on_return_callback(self.publish_message_to_dlq_sqs)

      self.channel.exchange_declare(
         exchange=self.connection_parameter["exchange"],
         exchange_type=self.connection_parameter["exchange_type"],
         durable=True
      )

      self.channel.queue_declare(
         queue=self.connection_parameter["queue_name"],
         durable=True
      )

      self.channel.queue_bind(
         exchange=self.connection_parameter["exchange"],
         queue=self.connection_parameter["queue_name"],
         routing_key=self.connection_parameter["routing_key"]
      )

   def publish_message(self):
      pass




