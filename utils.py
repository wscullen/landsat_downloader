import sys
import os
from collections import namedtuple

from multiprocessing.dummy import Pool as ThreadPool
import multiprocessing

import logging

logger = logging.getLogger(__name__)

class HiddenPrints:
    """Small utility class to suppress 3rd party print statements

    This class allows redirect of the stdout so you can suppress unwanted
    print statements from 3rd party libraries. Using the built in funcs
    __enter__ and __exit__, you can easily set up a block of code to
    suppress the print statements with the ``with`` keyword.

    Example:
        Use the with keyword to start suppressing print statements::

            with HiddenPrints():
                print("Does NOT go to stdout")

            print("DOES go to stdout")

    .. note::
        This is just a small utility class and can safely be ignored. Note,
        this is just a test of a note.

    """

    def __enter__(self):
        self._original_stdout = sys.stdout
        sys.stdout = open(os.devnull, 'w')

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout = self._original_stdout


TaskStatus = namedtuple('TaskStatus', ['status', 'message', 'data'])

# Code below is from django example here:
# https://stackoverflow.com/questions/18319101/whats-the-best-way-to-generate-random-strings-of-a-specific-length-in-python

import random
import hashlib
import time

SECRET_KEY = 'MY SECRET KEY IS HERE IT IS FOR MAKING A HASH UNIQUE OH BOY!'

try:
    random = random.SystemRandom()
    using_sysrandom = True
except NotImplementedError:
    import warnings
    warnings.warn('A secure pseudo-random number generator is not available '
                  'on your system. Falling back to Mersenne Twister.')
    using_sysrandom = False


def get_random_string(length=12,
                      allowed_chars='abcdefghijklmnopqrstuvwxyz'
                                    'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'):
    """
    Returns a securely generated random string.

    The default length of 12 with the a-z, A-Z, 0-9 character set returns
    a 71-bit value. log_2((26+26+10)^12) =~ 71 bits
    """
    if not using_sysrandom:
        # This is ugly, and a hack, but it makes things better than
        # the alternative of predictability. This re-seeds the PRNG
        # using a value that is hard for an attacker to predict, every
        # time a random string is required. This may change the
        # properties of the chosen random sequence slightly, but this
        # is better than absolute predictability.
        random.seed(
            hashlib.sha256(
                ("%s%s%s" % (
                    random.getstate(),
                    time.time(),
                    SECRET_KEY)).encode('utf-8')
            ).digest())
    return ''.join(random.choice(allowed_chars) for i in range(length))


def abortable_worker(func, *args, **kwargs):
    timeout = kwargs.get('timeout', None)
    p = ThreadPool(1)
    res = p.apply_async(func, args=args)
    try:
        out = res.get(timeout)  # Wait timeout seconds for func to complete.
        return out
    except multiprocessing.TimeoutError:
        print("Aborting due to timeout", args[1])
        p.terminate()
        raise

def resultCallback(result):
    logger.debug("Got result {}".format(result))

def errorCallback(result):
    logger.critical("Got ERROR {}".format(result))
