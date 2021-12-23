from setuptools import setup
import os
from pathlib import Path

__version__ = "v1.0.6"

base_dir = os.path.dirname(os.path.realpath(__file__))
requirements_path = Path(base_dir, "requirements.txt")
install_requires = []
if os.path.isfile(requirements_path):
    with open(requirements_path) as f:
        install_requires = f.read().splitlines()

setup(
    name="landsat_downloader",
    version=__version__,
    description="Utilities for downloading Landsat and Sentinel products from USGS",
    url="https://github.com/sscullen/landsat_downloader.git",
    author="Shaun Cullen",
    author_email="ss.cullen@uleth.ca",
    license="MIT",
    packages=["landsat_downloader"],
    zip_safe=False,
    install_requires=install_requires,
)
