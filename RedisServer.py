import asyncio
import aioredis
import uvloop
import json
import logging
from aioredis.pubsub import Receiver
from aioredis.abc import AbcChannel
from typing import Coroutine
from Job import Job
from asyncio import Task

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


class RedisServer:

    def __init__(self):
        self.redis = None
        self.receiver = None
        self.loop = None
        self.path = None
        self.mpsc = None
        self.routes = {}

    async def handler(self):
        async for channel, msg in self.mpsc.iter():
            try:
                assert(isinstance(channel, AbcChannel))
                channel = str(channel.name, 'utf-8')
                coro = self.routes.get(channel)
                if coro:
                    msg = json.loads(str(msg, 'utf-8'))
                    job = Job.from_json(msg)
                    # assert(job.type == channel.name)
                    await coro(*job.args, **job.kwargs)
            except Exception as e:
                logging.exception('')

    async def start(self):
        self.redis = await aioredis.create_redis(self.path, loop=self.loop)
        self.mpsc = Receiver(loop=self.loop)
        Task(self.handler())
        await self.redis.subscribe(*[self.mpsc.channel(route) for route in self.routes])

    async def stop(self):
        self.redis.stop()
        self.receiver.stop()

    def route(self, path: str):
        def pathed(coro: Coroutine):
            self.routes[path] = coro
            return coro

        return pathed

    def serve(self, path, loop=asyncio.get_event_loop()):
        self.path = path
        self.loop = loop
        self.loop.create_task(self.start())
        self.loop.run_forever()


if __name__ == '__main__':
    app = RedisServer()

    @app.route('/')
    async def test(*args, **kwargs):
        print(args,kwargs)

    app.serve('redis://localhost')

