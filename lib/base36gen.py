#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016


'''
Generator for Base36 IDs.
'''


import itertools as it


class Base36Generator(object):
    '''
    Generator for consecutive base-36 IDs.
    '''

    alphabet = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'

    def __init__(self, start=0):
        # For each digit, keep a counter and the current character.
        # The digits are stored "backwards" (least-significant digit first).
        self.counters = [iter(self.alphabet)]
        self.current = [None]

        # Fast-forward the iterator if need be.
        for _ in range(start):
            next(self)

    def __iter__(self):
        return self

    def __next__(self):
        for i in it.count():
            # Iterate over the digits as far as necessary.
            # Most of the time, this loop does only 1 iteration.
            try:
                # Increment this digit. If successful, then we're done.
                self.current[i] = next(self.counters[i])
                break
            except StopIteration:
                # Overflow: Reset this digit.
                self.counters[i] = iter(self.alphabet)
                self.current[i] = next(self.counters[i])
                # No break here: another iteration is needed
                # to increment the next-higher digit.
            except IndexError:
                # A new digit is needed.
                # Start counting from 1, as we have to skip the implicit
                # "leading zero".
                self.counters.append(iter(self.alphabet[1:]))
                self.current.append(next(self.counters[i]))
                break
        # Format as a string in correct order.
        return self._format(self.current)

    def last(self):
        'Get the last-yielded value.'
        return self._format(self.current)

    @staticmethod
    def _format(digits):
        return ''.join(reversed(digits))
