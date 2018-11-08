"""
This module represents an implementation from Lab3 of a barrier.

Computer Systems Architecture Course
Assignment 1
March 2018
"""

from threading import Condition

class ReusableBarrier():
    def __init__(self, num_threads):
        self.num_threads = num_threads
        self.count_threads = self.num_threads
        self.cond = Condition()

    def wait(self):
        self.cond.acquire()
        self.count_threads -= 1;
        if self.count_threads == 0:
            self.cond.notify_all()
            self.count_threads = self.num_threads
        else:
            self.cond.wait()
        self.cond.release()
 