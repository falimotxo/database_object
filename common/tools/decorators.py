import functools
import logging
import signal
import time

from threading import Lock


def synchronized(lock: Lock):
    """Synchronization decorator."""

    def wrap(f):
        def new_function(*args, **kw):
            lock.acquire()
            try:
                return f(*args, **kw)
            finally:
                lock.release()

        return new_function

    return wrap


def dump_args(func):
    """This decorator dumps out the arguments passed to a function before calling it"""

    first_position_variable = 0
    if len(func.__code__.co_varnames) > 0 and func.__code__.co_varnames[0] == 'self':
        first_position_variable = 1

    argnames = func.__code__.co_varnames[first_position_variable:func.__code__.co_argcount]
    fname = func.__name__

    def echo_func(*args, **kwargs):
        print(fname, ':', ', '.join(
            '%s=%r' % entry for entry in list(zip(argnames, args[first_position_variable:])) + list(kwargs.items())
        )
              )
        return func(*args, **kwargs)

    return echo_func


class log_function(object):
    """Logging decorator that allows you to log with a specific logger."""

    def __init__(self, logger: logging.Logger = None, level: int = logging.INFO):
        self.logger = logger
        self.level = level

    def __call__(self, func):
        """
        Returns a wrapper that wraps func.
        The wrapper will log the entry and exit points of the function with logging.INFO level.
        """

        @functools.wraps(func)
        def wrapper(*args, **kwds):

            first_position_variable = 0
            if len(func.__code__.co_varnames) > 0 and func.__code__.co_varnames[0] == 'self':
                first_position_variable = 1

            argnames = func.__code__.co_varnames[first_position_variable:func.__code__.co_argcount]
            arguments = ', '.join(
                '%s=%r' % entry for entry in list(zip(argnames, args[first_position_variable:])) + list(kwds.items()))
            entry_message = 'Entering {} {}'.format(func.__name__,
                                                    '' if len(arguments) == 0 else 'with {}'.format(arguments))

            # func.__code__ contains all data from caller function
            real_filename = func.__code__.co_filename[func.__code__.co_filename.rfind('/') + 1:]
            real_lineno = func.__code__.co_firstlineno

            if not self.logger:
                print(entry_message)
            else:
                self.logger.log(self.level, entry_message, extra={'name_override': func.__name__,
                                                                  'file_override': real_filename,
                                                                  'lineno_override': real_lineno})

            start = time.perf_counter()
            f_result = func(*args, **kwds)
            elapsed_time = round((time.perf_counter() - start) * 1000, 2)

            exit_message = 'Exiting ({}ms) {} with [{}]'.format(elapsed_time, func.__name__, f_result)
            if not self.logger:
                print(exit_message)
            else:
                self.logger.log(self.level, exit_message, extra={'name_override': func.__name__,
                                                                 'file_override': real_filename,
                                                                 'lineno_override': real_lineno})

            return f_result

        return wrapper


def timeout(seconds, error_message='Function call timed out'):
    def decorated(func):
        def _handle_timeout(signum, frame):
            raise TimeoutError(error_message)

        def wrapper(*args, **kwargs):
            signal.signal(signal.SIGALRM, _handle_timeout)
            signal.alarm(seconds)
            try:
                result = func(*args, **kwargs)
            finally:
                signal.alarm(0)
            return result

        return functools.wraps(func)(wrapper)

    return decorated
