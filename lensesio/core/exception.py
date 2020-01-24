#!/usr/bin/env python3


class lenses_exception(Exception):
    def __init__(self, error):
        self.error = error
