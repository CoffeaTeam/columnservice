#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import os.path
from setuptools import (
    setup,
    find_packages,
)


about = {}
with open(os.path.join("columnservice", "version.py")) as f:
    exec(f.read(), about)


setup(
    name="columnservice",
    version=about["__version__"],
    packages=find_packages(exclude=("columnservice-server",)),
    include_package_data=True,
    description="A column management service for coffea",
    long_description=open("README.md", "rb").read().decode("utf8", "ignore"),
    long_description_content_type="text/markdown",
    maintainer="Nick Smith",
    maintainer_email="nick.smith@cern.ch",
    url="https://github.com/nsmith-/columnservice",
    download_url="https://github.com/nsmith-/columnservice/releases",
    license="BSD 3-clause",
    test_suite="tests",
    install_requires=["httpx==0.12", "minio"],
    extras_require={
        "server": [
            "motor==2.1.0",
            "fastapi==0.52.0",
            "uvicorn==0.11.3",
            "dmwmclient==0.0.4",
            "python-multipart==0.0.5",
        ],
        "dev": ["flake8", "black", "pytest-asyncio"],
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Topic :: Scientific/Engineering :: Physics",
    ],
)
