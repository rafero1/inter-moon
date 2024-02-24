import logging
import traceback
import coloredlogs


dateformat = '%m-%d-%Y %H:%M:%S'
filename = 'moon/log.txt'
fmt = '%(asctime)s - [%(levelname)s] \n\t%(message)s'
stream_handler = logging.StreamHandler()
formatter = coloredlogs.ColoredFormatter(fmt)
stream_handler.setFormatter(formatter)

logging.basicConfig(
    level=logging.INFO,
    format=fmt,
    datefmt=dateformat,
    handlers=[
        logging.FileHandler(filename),
        stream_handler
    ]
)


def request(request, response_time):
    logging.info(
        'Request {}: Response Time {}'.format(
            request.q_query, response_time
        )
    )


def i(module, msg):
    logging.info('[{}] {}'.format(module, msg))


def e(module, exc_info):
    if exc_info:
        err_class = str(exc_info[0].__name__)
        msg_err = exc_info[1]
        trace_err = exc_info[2]
        trace_err = ''.join(traceback.format_tb(trace_err))
        trace_err = trace_err.replace('\n', '\n\t')
        log_message = '\n\t{}: {}\n\tTraceback:\n\t{}'.format(
            err_class,
            msg_err,
            trace_err
        )
        logging.error('[{}] {}'.format(module, log_message))
