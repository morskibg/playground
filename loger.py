from os.path import join
import logging
import socket
from settings import BENCH_LOG_PATH


def get_logger(name):
    
    extra = {'hostname': socket.gethostname(), 'ip': socket.gethostbyname(socket.gethostname())}    
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler(join(BENCH_LOG_PATH, 'bench.log'))
    fh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('date_time:%(asctime)s # name:%(name)s # leve_lname:%(levelname)s # host_Sname:%(hostname)s # ip:%(ip)s # %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(ch)
    logger = logging.LoggerAdapter(logger, extra)

    return logger
