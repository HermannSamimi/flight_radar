# setup.py

from setuptools import setup, find_packages

setup(
    name="flight_radar",
    version="0.1.0",
    description="Live flight tracking and ingestion pipelines",
    packages=find_packages(),
    install_requires=[
        "requests",
        "kafka-python",
        "redis",
        "python-dotenv",
        "prometheus-client",
        "python-json-logger",
        "snowflake-connector-python",
        "python-telegram-bot",
        # (and any other libs you actually use)
    ],
)