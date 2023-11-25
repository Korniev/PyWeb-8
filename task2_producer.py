from datetime import datetime
import json

import pika
from mongoengine import connect
from task2_models import Contact

connect(db='Go-it_HW8', host="mongodb://localhost:27017")


credentials = pika.PlainCredentials('guest', 'guest')
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost', port=5672, credentials=credentials))
channel = connection.channel()

channel.exchange_declare(exchange='korniev exchange', exchange_type='direct')
channel.queue_declare(queue='korniev_queue', durable=True)
channel.queue_bind(exchange='korniev exchange', queue='korniev_queue')


def create_tasks(nums: int):
    for i in range(nums):
        contact = Contact(fullname=f"Name {i}", email=f"user{i}@example.com")
        contact.save()

        message = {
            'id': str(contact.id),
            'payload': f"Date: {datetime.now().isoformat()}"
        }

        channel.basic_publish(exchange='korniev exchange', routing_key='korniev_queue',
                              body=json.dumps(message).encode())

    connection.close()


if __name__ == '__main__':
    create_tasks(100)
