#!/usr/bin/env python3


class lenses_exception(BaseException):
    def __init__(self, value):
        self.value = value
  
    def __str__(self):
        return(repr(self.value))
        