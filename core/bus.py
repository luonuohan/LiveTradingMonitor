import asyncio
from collections import defaultdict

class EventBus:
    def __init__(self):
        self._subs = defaultdict(list)

    def subscribe(self, topic: str, queue: asyncio.Queue):
        self._subs[topic].append(queue)

    async def publish(self, topic: str, data):
        for q in self._subs.get(topic, []):
            await q.put(data)

BUS = EventBus()
