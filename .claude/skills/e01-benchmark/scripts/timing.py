"""互斥阶段计时：同一时刻只允许一个阶段，杜绝重叠相加。

阶段由同一计时器累加，彼此不重叠；`unaccounted_ms` 是总墙钟里未归入任何阶段的
部分（进程启动、导入、GC 等），不得把它猜成某个阶段。缺失的阶段不出现在结果里，
调用方不得用 0 代替未测。
"""

import contextlib
import time


class PhaseTimer:
    def __init__(self):
        self.phases = {}
        self._active = None
        self._origin = time.monotonic()

    @contextlib.contextmanager
    def phase(self, name):
        if self._active is not None:
            raise ValueError("nested_phase_not_allowed")
        self._active = name
        started = time.monotonic()
        try:
            yield
        finally:
            self.phases[name] = self.phases.get(name, 0.0) + (time.monotonic() - started) * 1000.0
            self._active = None

    def report(self):
        total = (time.monotonic() - self._origin) * 1000.0
        accounted = sum(self.phases.values())
        return {"unit": "ms", "clock": "monotonic",
                "phases": {name: round(value, 3) for name, value in self.phases.items()},
                "phases_sum_ms": round(accounted, 3),
                "total_wall_ms": round(total, 3),
                "unaccounted_ms": round(total - accounted, 3),
                "containment": "各阶段由同一计时器互斥累加，互不重叠；不得把阶段相加当作总时长"}


def rate(numerator, denominator_ms):
    """固定分母的速率；分母缺失或为零时返回 None，不填 0。"""
    if numerator is None or not denominator_ms:
        return None
    return round(numerator / (denominator_ms / 1000.0), 3)
