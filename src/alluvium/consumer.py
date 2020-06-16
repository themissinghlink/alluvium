import json
import logging
from abc import ABC, abstractmethod, abstractproperty
from typing import Type, TypeVar

import boto3


class CDCEventProcessor(ABC):
    """Encasulates post processing logic depending on the cdc format you expect to receive from the database."""

    def __init__(self, payload):
        self.cdc_event = json.loads(payload)

    @abstractproperty
    def should_keep(self):
        pass

    @abstractmethod
    def post_process(self):
        pass


class Wal2JsonProcessorV1(CDCEventProcessor):
    """
    Post processing logic for postgres WAL2JSON Format v1.

    Documentation: https://access.crunchydata.com/documentation/wal2json/2.0/
    """

    def __init__(self, payload):
        super(Wal2JsonProcessorV1, self).__init__(payload)

    @property
    def should_keep(self):
        return bool(self.cdc_event["change"])

    def post_process(self):
        return self.cdc_event


class Wal2JsonProcessorV2(CDCEventProcessor):
    """
    Post processing logic for postgres WAL2JSON Format v2.

    Documentation: https://access.crunchydata.com/documentation/wal2json/2.0/
    """

    IGNORE_ACTIONS = {"B", "C"}

    def __init__(self, payload):
        super(Wal2JsonProcessorV2, self).__init__(payload)

    @property
    def should_keep(self):
        return self.cdc_event["action"] not in self.IGNORE_ACTIONS

    def post_process(self):
        return self.cdc_event


ProcessorType = TypeVar("CDCEventProcessor", bound=CDCEventProcessor)


class Consumer(ABC):
    """Defines the consumer abstract class used by psycopg2's consume_stream."""

    def __init__(self, processor: Type[ProcessorType]):
        self.processor = processor

    @abstractmethod
    def __call__(self, consumable):
        pass


class KinesisFirehoseConsumer(Consumer):
    """Passes post processed CDC Events to Kinesis."""

    def __init__(self, delivery_stream_name: str, cdc_event_processor: Type[ProcessorType]):
        super(KinesisFirehoseConsumer, self).__init__(cdc_event_processor)
        self.client = boto3.client("firehose")
        self.delivery_stream_name = delivery_stream_name

    def __call__(self, consumable):
        cdc_event = self.processor(consumable.payload)
        if cdc_event.should_keep:
            self.client.put_record(
                DeliveryStreamName=self.delivery_stream_name,
                Record={"Data": json.dumps(cdc_event.post_process())},
            )


class LogConsumer(Consumer):
    """Passes post processed CDC Events to logger."""

    def __init__(self, cdc_event_processor: Type[ProcessorType]):
        super(LogConsumer, self).__init__(cdc_event_processor)

    def __call__(self, consumable):
        cdc_event = self.processor(consumable.payload)
        if cdc_event.should_keep:
            logging.info(cdc_event.post_process())
