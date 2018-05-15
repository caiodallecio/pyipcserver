import hashlib
import time
from json import dumps


class JobFromJsonError(Exception):
    def __init__(self, data):
        self.data = data

    def __str__(self):
        return 'Failed to convert json to Job obj {}'.format(self.data)


class Job:
    hash = hashlib.md5()

    def __init__(self, function_name, message_type, return_channel, args, kwargs):
        self.time = time.time()
        self.function_name = function_name
        self.message_type = message_type
        self.return_channel = return_channel
        self.args = args
        self.kwargs = kwargs
        Job.hash.update(bytes(str(time.time() + id(self)), encoding='utf-8'))
        self.id = Job.hash.hexdigest()

    def __repr__(self):
        return '<class {} Job({},{},{},{},{},{})>'.format(self.id, self.function_name, self.message_type, self.return_channel, self.args, self.kwargs)

    def __str__(self):
        return repr(self)

    @property
    def delta_time(self):
        return time.time() - self.time

    @property
    def json(self):
        return dumps(self.__dict__)

    @staticmethod
    def from_json(data: dict):
        function_name = data.get('function_name')
        message_type = data.get('message_type')
        return_channel = data.get('return_channel')
        job_time = data.get('time')
        job_id = data.get('id')
        args = data.get('args')
        kwargs = data.get('kwargs')

        if all((function_name,message_type,return_channel,job_time,job_id)) and (args or kwargs):
            job = Job(function_name, message_type, return_channel,args,kwargs)
            job.time = job_time
            job.j_id = job_id
            return job
        else:
            raise JobFromJsonError(data)


if __name__ == '__main__':
    print(Job('test', 'test', 'test', test='test', test0='test').json)

