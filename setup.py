#!/usr/bin/env python

from setuptools import setup

install_requires = ['drive-ami',
                    'drive-casa',
                    ]

setup(
    name="autocrunch",
    version="0.2.2",
    packages=['autocrunch'],
    description="Automatically process incoming files.",
    author="Tim Staley",
    author_email="timstaley337@gmail.com",
    url="https://github.com/timstaley/autocrunch",
    install_requires=install_requires,
)
