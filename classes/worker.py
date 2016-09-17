import threading


class Worker(threading.Thread):
    """Thread executing tasks from a given tasks queue"""

    def __init__(self, tasks, result_output):
        super(Worker, self).__init__()
        self.tasks = tasks
        self.result_output = result_output
        self.daemon = True
        self.start()

    def run(self):
        while True:
            func, args, kwargs = self.tasks.get()
            try:
                self.result_output.append(func(*args, **kwargs))
            except Exception:
                import traceback, sys
                _, _, tb = sys.exc_info()
                traceback.print_tb(tb)
            self.tasks.task_done()
