from Queue import Queue

from worker import Worker


class ThreadPool(object):
    """Pool of threads consuming tasks from a queue"""
    def __init__(self, max_threads):
        self.tasks = Queue(max_threads)
        for _ in xrange(max_threads):
            Worker(self.tasks)

    def add_task(self, function, *args, **kargs):
        """Add a task to the queue"""
        self.tasks.put((function, args, kargs))

    def wait_completion(self):
        """Wait for completion of all the tasks in the queue"""
        self.tasks.join()