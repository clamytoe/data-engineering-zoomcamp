import abc
import csv
from time import sleep
from typing import Dict

from kafka import KafkaProducer
from settings import (
    CONFIG,
    FHV_DATA_PATH,
    GREEN_DATA_PATH,
    PRODUCE_FHV_TOPIC_RIDES_CSV,
    PRODUCE_GREEN_TOPIC_RIDES_CSV,
)


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
        return
    message = f"Record {msg.key()} successfully produced to "
    message += f"{msg.topic()} [{msg.partition()}] at offset "
    message += f"{msg.offset()}"
    print(message)


class CSVProducer(metaclass=abc.ABCMeta):
    def __init__(self, props: Dict):
        self.producer = KafkaProducer(**props)

    @abc.abstractstaticmethod
    def read_records(self):
        pass

    def publish(self, topic: str, records: [str, str]):
        for key_value in records:
            key, value = key_value
            try:
                self.producer.send(topic=topic, key=key, value=value)
                print(f"Producing record for <key: {key}, value:{value}>")
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Exception while producing record - {value}: {e}")

        self.producer.flush()
        sleep(1)


class FHVCSVProducer(CSVProducer):
    @staticmethod
    def read_records(resource_path: str):
        records, ride_keys = [], []
        # i = 0
        with open(resource_path, "r") as f:
            reader = csv.reader(f)
            _ = next(reader)  # skip the header
            for row in reader:
                # dispatching_base_num, pickup_datetime, dropOff_datetime, PUlocationID, DOlocationID
                # SR_Flag, Affiliated_base_number
                records.append(
                    f"{row[0]}, {row[1]}, {row[2]}, {row[3]}, {row[4]}, {row[5]}, {row[6]}"
                )
                ride_keys.append(str(row[0]))
                # i += 1
                # if i == 5:
                #     break
        return zip(ride_keys, records)


class GreenCSVProducer(CSVProducer):
    @staticmethod
    def read_records(resource_path: str):
        records, ride_keys = [], []
        # i = 0
        with open(resource_path, "r") as f:
            reader = csv.reader(f)
            _ = next(reader)  # skip the header
            for row in reader:
                # VendorID, lpep_pickup_datetime, lpep_dropoff_datetime, store_and_fwd_flag
                # RatecodeID, PULocationID, DOLocationID, passenger_count, trip_distance
                # fare_amount, extra, mta_tax, tip_amount, tolls_amount, ehail_fee
                # improvement_surcharge, total_amount, payment_type, trip_type, congestion_surcharge
                records.append(
                    f"{row[0]}, {row[1]}, {row[2]}, {row[3]}, {row[4]}, {row[5]}, {row[6]}, \
                    {row[7]}, {row[8]}, {row[9]}, {row[10]}, {row[11]}, {row[12]}, {row[13]}, \
                    {row[14]}, {row[15]}, {row[16]}, {row[17]}, {row[18]}, {row[19]}"
                )
                ride_keys.append(str(row[0]))
                # i += 1
                # if i == 5:
                #     break
        return zip(ride_keys, records)


if __name__ == "__main__":
    fhv_producer = FHVCSVProducer(props=CONFIG)
    green_producer = GreenCSVProducer(props=CONFIG)
    fhv_ride_records = fhv_producer.read_records(resource_path=FHV_DATA_PATH)
    green_ride_records = green_producer.read_records(resource_path=GREEN_DATA_PATH)
    print(fhv_ride_records)
    print(green_ride_records)
    fhv_producer.publish(topic=PRODUCE_FHV_TOPIC_RIDES_CSV, records=fhv_ride_records)
    green_producer.publish(
        topic=PRODUCE_GREEN_TOPIC_RIDES_CSV, records=green_ride_records
    )
