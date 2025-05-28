import logging

class OneTimeLogger:
    def __init__(self, base_logger):
        self.logger = base_logger
        self.seen_messages = set()

    def info(self, msg):
        self.logger.info(msg)

    def debug(self, msg):
        self.logger.debug(msg)

    def warning(self, msg):
        self.logger.warning(msg)

    def error(self, msg):
        self.logger.error(msg)

    def info_once(self, msg):
        if msg not in self.seen_messages:
            self.logger.info(msg)
            self.seen_messages.add(msg)

base_logger = logging.getLogger("BaseAgent")
one_time_logger = OneTimeLogger(base_logger)