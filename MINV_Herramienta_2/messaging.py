"""RabbitMQ: confirmaciones de publicación y ACK manual del consumidor."""
import json
import os
import time
import traceback
from core import relay_once, consume_one, request

QUEUE = 'minv.reservations.v1'


def connect():
    import pika
    params = pika.URLParameters(os.environ['AMQP_URL'])
    params.heartbeat = 30
    params.blocked_connection_timeout = 5
    params.socket_timeout = 5
    params.stack_timeout = 10
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE, durable=True)
    return connection, channel


def relay(path):
    import pika
    while True:
        connection = None
        try:
            connection, channel = connect()
            channel.confirm_delivery()
            def publish(payload):
                channel.basic_publish(exchange='', routing_key=QUEUE, body=payload,
                    properties=pika.BasicProperties(delivery_mode=2, content_type='application/json'),
                    mandatory=True)
            while True:
                relay_once(path, publish)
                connection.process_data_events(time_limit=0.05)
        except Exception:
            traceback.print_exc()
            time.sleep(1)
        finally:
            if connection and connection.is_open:
                connection.close()


def consumer(path):
    while True:
        connection = None
        try:
            connection, channel = connect()
            channel.basic_qos(prefetch_count=1)
            def callback(ch, method, properties, body):
                try:
                    payload = json.loads(body)
                    time.sleep(float(os.environ.get('PROCESSING_MS', '10')) / 1000)
                    consume_one(path, payload, lambda order_id: request(
                        os.environ['ORDERS_URL'] + '/complete', {'id': order_id}))
                except Exception:
                    traceback.print_exc()
                    time.sleep(0.2)
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                else:
                    ch.basic_ack(delivery_tag=method.delivery_tag)
            channel.basic_consume(queue=QUEUE, on_message_callback=callback, auto_ack=False)
            channel.start_consuming()
        except Exception:
            traceback.print_exc()
            time.sleep(1)
        finally:
            if connection and connection.is_open:
                connection.close()
