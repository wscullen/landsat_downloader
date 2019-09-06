# -*- coding: utf-8 -*-
"""runningtime.py -- Run time Module

This module is used to make measuring the run time of a specific function or
section of code in general more convenient. Simply create the runtime object
to start the timer, and call the finish() method to stop the timer. The
str representation of the object will output a summary of the time taken.

Example:
    Import and use the RunTime class::

     import runtime as RunTime

     job_time = RunTime()

     ... some code or function calls ...

     job_time.finish()

     print('The job took:', str(job_time))

     ... print to screen total seconds, # of hours, and # of minutes


.. topic:: Contact

    Shaun Cullen (shaun [dot] cullen [at] canada [dot] ca)
    January 24 2018
    https://github.com/sscullen/sentinel2dd

"""

import time

import logging
logger = logging.getLogger("simpleExample")


class RunningTime:
    """Simple class that measures elapsed running time."""

    def __init__(self):
        """Constructor func starts the timer by capturing the present time."""

        logger.debug("Creating RunTime object, capturing start time")
        self.start_time = time.time()

    def finish(self):
        """Finish func stops the timer by capturing the present time.

        Using the start_time captured when the object is created
        and the finish_time when finish() is called, an elapsed_time in
        seconds can be calculated and stored. The hours, mins, seconds
        disgarding remainders, are also calculated and stored.

        """
        self.finish_time = time.time()
        self.elapsed_time = self.finish_time - self.start_time

        self.hours = int(self.elapsed_time // (60 * 60))
        self.mins = int((self.elapsed_time // 60) % 60)
        self.seconds = int(self.elapsed_time
                            - (self.hours * 3600)
                            - (self.mins * 60))

    def restart(self):
        """Start a timer without having to create a new object"""

        self.start_time = time.time()
        self.finish_time = None

    def current_time(self):
        """Get a current update about the elapsed time so far

        Returns:
            (float): Returns the current elapsed time. Call finish() for a
            string representation.

        """

        current_time = time.time()
        self.elapsed_time = current_time - self.start_time

        self.hours = int(self.elapsed_time // (60 * 60))
        self.mins = int((self.elapsed_time // 60) % 60)
        self.seconds = int(self.elapsed_time
                           - (self.hours * 3600)
                           - (self.mins * 60))

        return self.elapsed_time


    def __str__(self):
        """String repr outputs a nicely formatted output of the total time.

        The function checks to make sure finish_time exists before trying to
        output the elapsed time, especially important if the user calls
        restart() and forgets to call finish()

        """

        if self.finish_time:
            return "RunTime: Total seconds: {}, Hours: {}, Minutes: {}, Seconds: {}"\
                .format(self.elapsed_time, self.hours, self.mins, self.seconds)
        else:
            return "RunTime: Task not finished yet. (call finish())"



if __name__ == '__main__':
    pass
