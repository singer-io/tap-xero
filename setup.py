#!/usr/bin/env python
from setuptools import setup, find_packages

setup(name="tap-xero",
      version="2.3.0",
      description="Singer.io tap for extracting data from the Xero API",
      author="Stitch",
      url="http://singer.io",
      classifiers=["Programming Language :: Python :: 3 :: Only"],
      py_modules=["tap_xero"],
      install_requires=[
          "singer-python==5.13.0",
          "requests==2.31.0",
      ],
      extras_require={
          'dev': [
              'ipdb',
              'pylint',
              'nose'
          ]
      },
      entry_points="""
          [console_scripts]
          tap-xero=tap_xero:main
      """,
      packages=["tap_xero"],
      package_data = {
          "schemas": ["tap_xero/schemas/*.json"]
      },
      include_package_data=True,
)
