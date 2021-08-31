#! /usr/bin/env python

import re
import os
from setuptools import setup, find_packages

base_path = os.path.dirname(__file__)

with open(os.path.join(base_path, "zolo", "version.py")) as fp:
    VERSION = re.compile(""".*__version__ = ["'](.*?)['"]""") \
        .match(fp.read()) \
        .group(1)

with open("README.md", "r") as fh:
    long_description = fh.read()


setup(
    name='zolo',
    version=VERSION,
    packages=find_packages(),
    url='https://github.com/firefirer1983/zolo.git',
    license='',
    author='张旭毅',
    author_email='fyman.zhang@gmail.com',
    description='梭罗虚拟货币量化交易框架',
    long_description=long_description,
    long_description_content_type="text/markdown",
    install_requires=[
        'huobi_restful @ git+ssh://git@github.com/firefirer1983/huobi_restful.git@main#egg=huobi_restful',
        'SQLAlchemy==1.3.20',
    ],
    dependency_links=[
        'git+ssh://git@github.com/firefirer1983/huobi_restful.git@main#egg=huobi_restful'
    ]
)
