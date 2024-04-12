import time

from kafka import KafkaProducer
import pandas as pd

# Create a producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')


def read_data_from_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, dtype='unicode')
    return df


def kafka_topic_produce_to_df(df: pd.DataFrame):
    for _, row in df.iterrows():
        print(dict(row))
        producer.send("iot-data", bytes(str(dict(row)), 'utf-8'))

        time.sleep(0.5)


if __name__ == '__main__':
    kafka_topic_produce_to_df(read_data_from_csv('data/reduced_data_1.csv'))
