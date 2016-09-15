import threading


class Worker(threading.Thread):
    """Thread executing tasks from a given tasks queue"""

    def __init__(self, tasks):
        super(Worker, self).__init__()
        self.tasks = tasks
        self.daemon = True
        self.start()

    def run(self):
        while True:
            func, args, kwargs = self.tasks.get()
            try:
                func(*args, **kwargs)
            except Exception, e:
                print e
                raise e
            self.tasks.task_done()
