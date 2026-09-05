"""Executable PyFlink event-time window example for the pinned 1.18 lab."""

import time

from pyflink.common import Duration, Time, Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.window import TumblingEventTimeWindows


class CountWindow(ProcessWindowFunction):
    def process(self, key, context, elements):
        count = sum(1 for _ in elements)
        window = context.window()
        yield f"key={key} count={count} window={window.start}-{window.end}"


def main():
    environment = StreamExecutionEnvironment.get_execution_environment()
    environment.set_parallelism(1)
    now = int(time.time() * 1000)
    event_time = now - 5 * 60 * 1000
    rows = [("u1", "login_failed", event_time + index * 1000) for index in range(20)]
    stream = environment.from_collection(
        rows,
        type_info=Types.TUPLE([Types.STRING(), Types.STRING(), Types.LONG()]),
    )
    watermarks = (
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5))
        .with_timestamp_assigner(lambda row, _: row[2])
    )
    (
        stream.assign_timestamps_and_watermarks(watermarks)
        .key_by(lambda row: row[0])
        .window(TumblingEventTimeWindows.of(Time.minutes(1)))
        .process(CountWindow(), output_type=Types.STRING())
        .print()
    )
    environment.execute("academy-event-time")


if __name__ == "__main__":
    main()
