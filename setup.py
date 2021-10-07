from setuptools import setup
from os import getenv

setup(
    name="sage_data_client",
    version=getenv("RELEASE_VERSION", "0.0.0"),
    description="Official Sage data client",
    url="https://github.com/sagecontinuum/sage-data-client",
    install_requires=[
        "pandas",
    ],
    packages=[
        "sage_data_client",
    ],
    python_requires=">=3.6",
)
