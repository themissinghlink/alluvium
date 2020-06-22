import json

import pytest

from alluvium.consumer import KinesisFirehoseConsumer, Wal2JsonProcessorV1, Wal2JsonProcessorV2


@pytest.fixture
def kinesis_delivery_stream():
    return "foo-stream"


class MockLogicalReplicationEvent(object):
    def __init__(self, consumable):
        self.payload = consumable


def test_wal_2_json_processor_v1():
    cdc_event = json.dumps({"change": [{"foo": "bar"}]})

    processor = Wal2JsonProcessorV1(cdc_event)
    assert processor.should_keep
    assert processor.post_process() == {"change": [{"foo": "bar"}]}

    cdc_event = json.dumps({"change": []})
    processor = Wal2JsonProcessorV1(cdc_event)
    assert not processor.should_keep


def test_wal_2_json_processor_v2():
    cdc_event = json.dumps({"action": "I", "foo": "bar"})
    processor = Wal2JsonProcessorV2(cdc_event)
    assert processor.should_keep
    assert processor.post_process() == {"action": "I", "foo": "bar"}

    cdc_event = json.dumps({"action": "B", "foo": "bar"})
    processor = Wal2JsonProcessorV2(cdc_event)
    assert not processor.should_keep


def test_kinesis_consumer(mocker, kinesis_delivery_stream):
    firehose = mocker.MagicMock()
    boto_calls = mocker.patch("alluvium.consumer.boto3.client", return_value=firehose)

    kinesis_consumer = KinesisFirehoseConsumer(
        kinesis_delivery_stream, cdc_event_processor=Wal2JsonProcessorV1
    )

    cdc_event = json.dumps({"change": [{"foo": "bar"}]})

    kinesis_consumer(MockLogicalReplicationEvent(cdc_event))
    firehose.put_record.assert_called_with(
        DeliveryStreamName=kinesis_delivery_stream, Record={"Data": cdc_event}
    )


def test_kinesis_consumer_empty_change(mocker, kinesis_delivery_stream):
    firehose = mocker.MagicMock()
    boto_calls = mocker.patch("alluvium.consumer.boto3.client", return_value=firehose)

    kinesis_consumer = KinesisFirehoseConsumer(
        kinesis_delivery_stream, cdc_event_processor=Wal2JsonProcessorV1
    )

    cdc_event = json.dumps({"change": []})

    kinesis_consumer(MockLogicalReplicationEvent(cdc_event))
    assert not firehose.put_record.called
