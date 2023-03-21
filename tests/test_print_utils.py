#!/usr/bin/env python
# encoding: utf-8

import unittest
from ihutilities import colour_text


class ColourTextTests(unittest.TestCase):
    def test_the_text_colours_are_correct(self):
        colours = [
            "red",
            "green",
            "blue",
            "cyan",
            "white",
            "yellow",
            "magenta",
            "grey",
            "black",
            "not available",
        ]
        print("\n", flush=True)
        for colour in colours:
            print(
                "{}: {} light: {}".format(
                    colour,
                    colour_text(colour, colour=colour),
                    colour_text(colour, colour="light_" + colour),
                ),
                flush=True,
            )
