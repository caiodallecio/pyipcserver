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

    def __init__(self, function, type, *args, **kwargs):
        self.time = time.time()
        self.function = function
        self.type = type
        self.args = args
        self.kwargs = kwargs
        Job.hash.update(bytes(str(time.time() + id(self)), encoding='utf-8'))
        self.id = Job.hash.hexdigest()

    def __repr__(self):
        return '<class {} Job({},{},{})>'.format(self.id, self.type, self.args, self.kwargs)

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
        j_type = data.get('type')
        args = data.get('args')
        kwargs = data.get('kwargs')

        j_time = data.get('time')
        j_id = data.get('id')

        if all((j_type, args, kwargs, j_time, j_id)):
            job = Job(j_type, args, kwargs)
            job.time = j_time
            job.j_id = j_id
            return job
        else:
            raise JobFromJsonError(data)


if __name__ == '__main__':
    print(Job('test', 'test', 'test', test='test', test0='test').json)

