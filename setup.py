from setuptools import setup, find_packages

setup(
    name="simple_data_dictionary",
    version="0.0.1",
    description="A simple data dictionary fo snowflake",
    packages=find_packages(),
    entry_points={
        "console_scripts": [
            # "task = snowflake_alerts:task_alerts.main",
            # "pipe = snowflake_alerts:pipe_alerts.main",
            # "cluster = snowflake_alerts:cluster_alerts.main",
        ]
    },
)
