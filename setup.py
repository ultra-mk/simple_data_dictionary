from setuptools import setup, find_packages

setup(
    name="simple_data_dictionary",
    version="0.0.1",
    description="A simple data dictionary fo snowflake",
    packages=find_packages(),
    entry_points={
        "console_scripts": [
            "fetch = simple_data_dictionary:snowflake_fetcher.main",
        ]
    },
)
