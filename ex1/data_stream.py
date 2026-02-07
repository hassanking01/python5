from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union


class DataStream(ABC):
    stream_type: str
    total_count: int

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        ...

    def filter_data(
            self, data_batch: List[Any],
            criteria: Optional[str] = None
                    ) -> List[Any]:
        if not criteria:
            return data_batch
        else:
            return [
                data for data in data_batch
                if isinstance(data, str) and data != credits
                    ]

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
            "Stream_type": self.stream_type,
            "total_count": self.total_count
        }


class SensorStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        self.stream_id = stream_id
        self.stream_type = "Sensor"

    def process_batch(
            self,
            data_batch: List[Dict[str, Union[int, float]]]
            ) -> str:
        result = "["
        total_tmp: Union[int, float] = 0
        tmp_count = 0
        for item in data_batch:
            key, data = item.popitem()
            item.update({key: data})
            if key == "tmp":
                total_tmp += data
                tmp_count += 1
            result += f"{key}:{data}, "
        try:
            self.avg_tmp = total_tmp / tmp_count
        except ZeroDivisionError:
            self.avg_tmp = 0
        result = result[:-2]
        result += "]"
        self.total_count = len(data_batch)
        return result

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        result = super().get_stats()
        result.update({"uniq_proce": self.avg_tmp})
        return result

    def filter_data(
            self,
            data_batch: List[Dict[str, Union[int, float]]],
            criteria:
            Optional[str] = None) -> List[Dict[str, Union[int, float]]]:
        if criteria:
            if criteria == "hot":
                result = []
                for item in data_batch:
                    (key, data) = item.popitem()
                    item.update({key: data})
                    if key == "tmp" and data >= 30:
                        result += [item]
                return result
            else:
                return data_batch
        else:
            return data_batch


class TransactionStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        self.stream_id = stream_id
        self.stream_type = "Transaction"

    def process_batch(self, data_batch: List[Dict[str, int]]) -> str:
        result = "["
        buy_total = 0
        sell_total = 0
        for item in data_batch:
            key, data = item.popitem()
            item.update({key: data})
            if key == "buy":
                buy_total += data
            elif key == "sell":
                sell_total += data
            result += f"{key}:{data}, "

        result = result[:-2]
        self.net_flow = buy_total - sell_total
        result += "]"
        self.total_count = len(data_batch)
        return result

    def filter_data(
            self,
            data_batch: List[Dict[str, Union[int, float]]],
            criteria:
            Optional[str] = None) -> List[Dict[str, Union[int, float]]]:
        if criteria:
            if criteria == "large":
                result = []
                for item in data_batch:
                    (key, data) = item.popitem()
                    item.update({key: data})
                    if data >= 100:
                        result += [item]
                return result
            else:
                return data_batch
        else:
            return data_batch

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        result = super().get_stats()
        result.update({"uniq_proce": self.net_flow})
        return result


class EventStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        self.stream_id = stream_id
        self.stream_type = "Event"

    def process_batch(self, data_batch: List[Dict[str, str]]) -> str:
        result = "["
        error_count = 0
        for item in data_batch:
            key, data = item.popitem()
            item.update({key: data})
            result += f"{key}, "
            if key == "error":
                error_count += 1
        result = result[:-2]
        result += "]"
        self.error_total = error_count
        self.total_count = len(data_batch)
        return result

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        result = super().get_stats()
        result.update({"uniq_proce": self.error_total})
        return result

    def filter_data(
            self,
            data_batch: List[Dict[str, str]],
            criteria:
            Optional[str] = None) -> List[Dict[str, str]]:
        if criteria:
            if criteria == "error":
                result = []
                for item in data_batch:
                    (key, data) = item.popitem()
                    item.update({key: data})
                    if key == criteria:
                        result += [item]
                return result
            else:
                return data_batch
        return data_batch


print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===\n")
print("Initializing Sensor Stream...")

sensor_id = "SENSOR_001"
sensor = SensorStream(sensor_id)
print(f"Stream ID: {sensor_id}, Type: Environmental Data")
sensor_data: list[dict[str, int | float]] = [
    {"tmp": 22.5},
    {"humidity": 65},
    {"pressure": 1013}
]
print("Processing sensor batch:", sensor.process_batch(sensor_data))
sensor_stats = sensor.get_stats()
print(f"{sensor_stats['Stream_type']} analysis: "
      f"{sensor_stats['total_count']} readings processed,"
      f" avg temp: {sensor_stats['uniq_proce']}°C")
print("\nInitializing Transaction Stream...")
transaction_id = "TRANS_001"
transaction = TransactionStream(transaction_id)
print(f"Stream ID: {transaction_id}, Type: Financial Data")
transaction_data = [{"buy": 100}, {"sell": 150}, {"buy": 75}]
print("Processing transaction batch:"
      f" {transaction.process_batch(transaction_data)}")
transaction_stats = transaction.get_stats()
sign = "+" if transaction_stats['uniq_proce'] else ""
print(f"{transaction_stats['Stream_type']} analysis: "
      f"{transaction_stats['total_count']} operations,"
      f" net flow: {sign}{transaction_stats['uniq_proce']} units")
print("\nInitializing Event Stream...")
event_id = "EVENT_001"
event = EventStream(event_id)
print(f"Stream ID: {event_id}, Type: System Events")
event_data = [
    {"login": "2026-02-05 09:15:23"},
    {"error": "2026-02-05 10:42:17"},
    {"logout": "2026-02-05 17:30:45"}
]
print("Processing transaction batch:"
      f" {event.process_batch(event_data)}")
event_stats = event.get_stats()
print(f"{event_stats['Stream_type']} analysis: "
      f"{event_stats['total_count']} events,"
      f" {event_stats['uniq_proce']} error detected")


class StreamProcessor:
    Batch_count: int = 0

    def pocess_streams(self, data: List[Any]) -> None:
        StreamProcessor.Batch_count += 1
        print(f"Batch {StreamProcessor.Batch_count} Results:")
        for item in data:
            process, data_batch = item
            process.process_batch(data_batch)
            result = process.get_stats()
            middle_str = ""
            if isinstance(process, SensorStream):
                middle_str = "readings"
            elif isinstance(process, TransactionStream):
                middle_str = "operations"
            else:
                middle_str = "events"
            print(f"- {result['Stream_type']} data: {result['total_count']}"
                  f" {middle_str} processed")

    def filter_data(self, data: List[Any]) -> str:
        print("Stream filtering active: High-priority data only")
        event_count = 0
        transaction_count = 0
        sensor_count = 0
        result = "Filtered results:"
        for item in data:
            process, data_batch = item
            if isinstance(process, SensorStream):
                filter_result = process.filter_data(data_batch, "hot")
                if filter_result:
                    sensor_count += len(filter_result)
            elif isinstance(process, EventStream):
                event_result: List[Dict[str, str]] = process.filter_data(
                        data_batch,
                        "error"
                )
                if event_result:
                    event_count += len(event_result)
            elif isinstance(process, TransactionStream):
                filter_result = process.filter_data(data_batch, "large")
                if filter_result:
                    transaction_count += len(filter_result)
        if sensor_count:
            result += f" {sensor_count} high temperature's,"
        if event_count:
            result += f" {event_count} critical sensor alerts,"
        if transaction_count:
            result += f" {transaction_count} large transaction"
        return result


print("\n=== Polymorphic Stream Processing ===")
print("Processing mixed stream types through unified interface...\n")
all_data: List[Any] = [
    (SensorStream("SENSOR_002"),
     [{"tmp": 22.5}, {"humidity": 65}, {"pressure": 1013}]),
    (TransactionStream("TRANS_002"),
     [{"buy": 150}, {"sell": 50}, {"buy": 75}, {"sell": 90}]),
    (EventStream("EVENT_002"),
        [
        {"login": "2026-02-05 09:15:23"},
        {"error": "2026-02-05 10:42:17"},
        {"error": "2026-02-06 02:52:10"},
        {"logout": "2026-02-05 17:30:45"}
    ])
]
stream_processor: StreamProcessor = StreamProcessor()
stream_processor.pocess_streams(all_data)
print()
print(stream_processor.filter_data(all_data))
print("\nAll streams processed successfully. Nexus throughput optimal.")
