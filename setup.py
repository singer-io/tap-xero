#!/usr/bin/env python
from setuptools import setup, find_packages

setup(name="tap-xero",
      version="0.1.29",
      description="Singer.io tap for extracting data from the Xero API",
      author="Stitch",
      url="http://singer.io",
      classifiers=["Programming Language :: Python :: 3 :: Only"],
      py_modules=["tap_xero"],
      install_requires=[
          "python-dateutil==2.6.0",  # This is required by singer-python,
          # without this being here explicitly, there are dependency issues
          "singer-python==3.5.0",
          "pyxero<1",
          "requests",
          "boto3",
      ],
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
