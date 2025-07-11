import queue
import threading

class DAGRunner:

    def __init__(self, max_workers: int = 4) -> None:
        self.max_workers = max_workers
        self.task_queue = queue.PriorityQueue()
        self.workers = []
        self.dags = {}
        self.running = False
        self.lock = threading.RLock()

        
