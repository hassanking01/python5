from abc import ABC, abstractmethod
from typing import Any, List


class DataProcessor(ABC):

    @abstractmethod
    def process(self, data: Any) -> str:
        ...

    @abstractmethod
    def validate(self, data: Any) -> bool:
        ...

    def format_output(self, result: str) -> str:
        return "Output: "


class NumericProcessor(DataProcessor):
    def process(self, data: List) -> str:
        sum_ = sum(data)
        len_ = len(data)
        return f"Processed {len_} numeric values, sum={sum_}, avg={sum_/len_}"

    def validate(self, data: List) -> bool:
        for num in data:
            cls_name = num.__class__.__name__
            if not cls_name == "int":
                return False
        return True

    def format_output(self, result: str) -> str:
        return super().format_output(result) + result


class TextProcessor(DataProcessor):

    def process(self, data: str) -> str:
        wordscount_ = len(text_data.split(' '))
        len_ = len(data)
        return f"Processed text: {len_} characters, {wordscount_} words"

    def validate(self, data: str) -> bool:
        if not data.__class__.__name__ == "str":
            return False
        return True

    def format_output(self, result: str) -> str:
        return super().format_output(result) + result


class LogProcessor(DataProcessor):
    def process(self, data: str) -> str:
        logs_prefex = {"ERROR": "[ALERT]", "INFO": "[INFO]"}
        log = data.split(':')[0]
        message = data.split(':')[1]
        return f"{logs_prefex[log]} {log} level detected:{message}"

    def validate(self, data: str) -> bool:
        if data.__class__.__name__ == "str":
            if "ERROR" in data or "INFO" in data:
                return True
        return False

    def format_output(self, result: str) -> str:
        return super().format_output(result) + result


print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")
print("Initializing Numeric Processor...")
numeric_proc = NumericProcessor()
numeric_data = [1, 2, 3, 4, 5]
print("Processing data:", numeric_data)
if numeric_proc.validate(numeric_data):
    print("Validation: Numeric data verified")
    result = numeric_proc.process(numeric_data)
    print(numeric_proc.format_output(result=result))

print("\nInitializing Text Processor...")
text_proc = TextProcessor()
text_data = "Hello Nexus World"
print("Processing data:", text_data)
if text_proc.validate(text_data):
    print("Validation: Text data verified")
    result = text_proc.process(text_data)
    print(text_proc.format_output(result))

print("\nInitializing Log Processor...")
log_proc = LogProcessor()
log_data = "ERROR: Connection timeout"
print("Processing data:", log_data)
if log_proc.validate(log_data):
    print("Validation: Log entry verified")
    result = log_proc.process(log_data)
    print(log_proc.format_output(result))

print()
processors = [
    (NumericProcessor(), [1, 2, 3]),
    (TextProcessor(), "hello world"),
    (LogProcessor(), "INFO: System ready")
]
i = 1
print("=== Polymorphic Processing Demo ===")
print("Processing multiple data types through same interface...")
for proc in processors:
    processor, data = proc
    if processor.validate(data):
        result = processor.process(data)
        print(f"Result {i}: {processor.format_output(result)}")
    i += 1
print("\nFoundation systems online. Nexus ready for advanced streams.")
