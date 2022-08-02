from __future__ import absolute_import

import abc


class AbstractType(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def encode(self, value): # pylint: disable=no-self-argument
        pass

    @abc.abstractmethod
    def decode(self, data): # pylint: disable=no-self-argument
        pass

    @classmethod
    def repr(cls, value):
        return repr(value)
