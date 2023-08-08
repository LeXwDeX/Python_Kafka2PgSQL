import logging
import queue
import threading
from concurrent.futures import ThreadPoolExecutor
from logging.handlers import TimedRotatingFileHandler


class AsyncLogHandler(logging.Handler):
    def __init__(self, log_file_path):
        super().__init__()
        self.log_queue = queue.Queue()
        self.executor = ThreadPoolExecutor(max_workers=5)
        self.handler = TimedRotatingFileHandler(log_file_path, when='midnight', encoding='utf-8')
        self.lock = threading.Lock()
        
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.handler.setFormatter(formatter)
    
    def emit(self, record):
        self.log_queue.put(record)
    
    def write_log(self, record):
        with self.lock:
            self.handler.emit(record)
    
    def handle(self, record):
        self.executor.submit(self.write_log, record)
    
    def flush(self):
        while not self.log_queue.empty():
            try:
                record = self.log_queue.get_nowait()
            except queue.Empty:
                break
            else:
                self.handle(record)


class AsyncLogger:
    def __init__(self, log_file_path):
        self.handler = AsyncLogHandler(log_file_path)
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        self.logger.addHandler(self.handler)
    
    def log(self, level, msg):
        self.logger.log(level, msg)
        self.handler.flush()

# logger = AsyncLogger('./log/log_file_path.log')
# logger.log(logging.DEBUG, 'This is a debug message.')
# logger.log(logging.INFO, 'This is an info message.')
# logger.log(logging.WARNING, 'This is a warning message.')
# logger.log(logging.ERROR, 'This is an error message.')
# logger.log(logging.CRITICAL, 'This is a critical message.')
