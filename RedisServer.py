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

    def __init__(self, channel:str):
        self.redis = None
        self.receiver = None
        self.loop = None
        self.path = None
        self.mpsc = None
        self.redis_a = None
        self.channel = channel
        self.routes = {}

    async def handler(self):
        async for channel, msg in self.mpsc.iter():
            try:
                assert(isinstance(channel, AbcChannel))
                channel = str(channel.name, 'utf-8')
                if channel == self.channel:
                    msg = json.loads(str(msg, 'utf-8'))
                    job = Job.from_json(msg)
                    coro = self.routes.get(job.function_name)
                    if coro:
                        await coro(*job.args, **job.kwargs)
            except Exception as e:
                logging.exception('')

    async def start(self,rpc=True):
        self.redis = await aioredis.create_redis(self.path, loop=self.loop)
        self.redis_a = await aioredis.create_redis(self.path,loop=self.loop)
        self.mpsc = Receiver(loop=self.loop)
        Task(self.handler())
        await self.redis.subscribe(self.mpsc.channel(self.channel))
        Task(app.rpc('/teste/teste', 'teste', teste='teste'))

    async def stop(self):
        self.redis.stop()
        self.receiver.stop()

    async def rpc(self, function_path:str, *args, **kwargs):
        _, channel, function_name = function_path.split('/',2)
        job = Job('/' + function_name, 'call', self.channel, args, kwargs)
        await self.redis_a.publish(channel, job.json)

    def route(self, path: str):
        def pathed(coro: Coroutine):
            self.routes[path] = coro
            return coro
        return pathed

    def serve(self, path, rpc=True, loop=asyncio.get_event_loop()):
        self.path = path
        self.loop = loop
        self.loop.create_task(self.start(rpc=rpc))
        self.loop.run_forever()

if __name__ == '__main__':
    app = RedisServer('teste')

    @app.route('/teste')
    async def test(first, teste):
        print(first,teste)


    app.serve('redis://localhost')





