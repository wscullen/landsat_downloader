from setuptools import setup

__version__ = "v0.0.0"

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
)
