from confluent_kafka import Consumer
from confluent_kafka import KafkaError, KafkaException
from consumers.base_consumer import BaseConsumer
import pandas as pd
import sys
import json
import time

class ConsumeConsumer(BaseConsumer):
    def __init__(self, group_id):
        super().__init__(group_id)
        # 여러 개의 topic을 consume할 수 있음
        self.topics = ['apis.seouldata.rt-bicycle']

        conf = {'bootstrap.servers': self.BOOTSTRAP_SERVERS,
                'group.id': self.group_id,
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': 'false'
                }

        self.consumer = Consumer(conf)
        # 특정 topic을 컨슘하겠다고 구독하는 절차..
        # on_assign -> on_assign 이벤트가 발생했을때 어떤 함수로 메세지를 처리할 것인지?
        # 즉, 특정 컨슈머가 특정 파티션에 대해 컨슘할때 어떤 로깅형식을 뿌려줄 것인지..?
        self.consumer.subscribe(self.topics, on_assign=self.callback_on_assign)


    def poll(self):
        try:
            while True:
	            # consume은 실제 브로커에서 메세지를 꺼내오겠다.
                msg_lst = self.consumer.consume(num_messages=100)
                if msg_lst is None or len(msg_lst) == 0: continue

                self.logger.info(f'message count:{len(msg_lst)}')
                for msg in msg_lst:
                    error = msg.error()
                    if error:
                        self.handle_error(msg, error)

                # 로직 처리 부분
                # Kafka 레코드에 대한 전처리, Target Sink 등 수행
                self.logger.info(f'message 처리 로직 시작')
                msg_val_lst = [json.loads(msg.value().decode('utf-8')) for msg in msg_lst]
                df = pd.DataFrame(msg_val_lst)
                print(df[:10])


                self.logger.info(f'message 처리 로직 완료, Async Commit 후 2초 대기')
                # 로직 처리 완료 후 Async Commit 수행
                self.consumer.commit(asynchronous=True)
                self.logger.info(f'Commit 완료')
                time.sleep(2)

        except KafkaException as e:
            self.logger.exception("Kafka exception occurred during message consumption")

        except KeyboardInterrupt: # Ctrl + C 눌러 종료시
            self.logger.info("Shutting down consumer due to keyboard interrupt.")

        finally:
            self.consumer.close()
            self.logger.info("Consumer closed.")


if __name__ == '__main__':
    consume_consumer= ConsumeConsumer('consume_consumer')
    consume_consumer.poll()