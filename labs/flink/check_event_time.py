"""Verify the event-time vs processing-time prediction objectively.

Runs the same 20 "5 minutes late" events through both a processing-time
tumbling window and an event-time tumbling window with a watermark, collects
the window labels each job actually produced (instead of just printing them,
like event_time.py does), and asserts the property you were asked to
predict: the event-time window's bucket boundary is ~5 minutes in the past,
while the processing-time window's bucket boundary is ~now.

Usage: python check_event_time.py
"""

import time

from pyflink.common import Duration, Time, Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.window import (
    TumblingEventTimeWindows,
    TumblingProcessingTimeWindows,
)

ONE_MINUTE_MS = 60_000


class WindowStart(ProcessWindowFunction):
    def process(self, key, context, elements):
        list(elements)  # drain
        yield context.window().start


def run_event_time(rows):
    environment = StreamExecutionEnvironment.get_execution_environment()
    environment.set_parallelism(1)
    stream = environment.from_collection(
        rows, type_info=Types.TUPLE([Types.STRING(), Types.STRING(), Types.LONG()])
    )
    watermarks = WatermarkStrategy.for_bounded_out_of_orderness(
        Duration.of_seconds(5)
    ).with_timestamp_assigner(lambda row, _: row[2])
    windowed = (
        stream.assign_timestamps_and_watermarks(watermarks)
        .key_by(lambda row: row[0])
        .window(TumblingEventTimeWindows.of(Time.minutes(1)))
        .process(WindowStart(), output_type=Types.LONG())
    )
    return list(windowed.execute_and_collect())


def run_processing_time(rows):
    environment = StreamExecutionEnvironment.get_execution_environment()
    environment.set_parallelism(1)
    stream = environment.from_collection(
        rows, type_info=Types.TUPLE([Types.STRING(), Types.STRING(), Types.LONG()])
    )
    windowed = (
        stream.key_by(lambda row: row[0])
        .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
        .process(WindowStart(), output_type=Types.LONG())
    )
    return list(windowed.execute_and_collect())


def main():
    now = int(time.time() * 1000)
    five_min_ago = now - 5 * ONE_MINUTE_MS
    rows = [("u1", "login_failed", five_min_ago + index * 1000) for index in range(20)]

    event_time_windows = run_event_time(rows)
    processing_time_windows = run_processing_time(rows)

    if not event_time_windows:
        raise AssertionError("event-time job produced no window output at all")
    if not processing_time_windows:
        raise AssertionError("processing-time job produced no window output at all")

    event_window_start = event_time_windows[0]
    processing_window_start = processing_time_windows[0]

    print(f"event-time window start:      {event_window_start} (now={now})")
    print(f"processing-time window start: {processing_window_start} (now={now})")

    event_age_ms = now - event_window_start
    processing_age_ms = now - processing_window_start

    if event_age_ms < 4 * ONE_MINUTE_MS:
        raise AssertionError(
            f"expected the event-time window to bucket ~5 minutes in the past, "
            f"but it started only {event_age_ms / 1000:.0f}s before now"
        )
    if processing_age_ms > ONE_MINUTE_MS:
        raise AssertionError(
            f"expected the processing-time window to bucket ~now, "
            f"but it started {processing_age_ms / 1000:.0f}s before now"
        )

    print(
        "PASS: identical events land in different window buckets depending on which "
        "clock the window uses — event-time buckets them where they actually happened "
        "(~5 minutes ago); processing-time buckets them where the job happened to be "
        "running (now). This is why a Flink job with no watermark strategy silently "
        "answers a different question than the one you asked for late-arriving data."
    )


if __name__ == "__main__":
    main()
