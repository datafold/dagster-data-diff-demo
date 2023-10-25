from setuptools import find_packages, setup

setup(
    name="data_diff_demo",
    packages=find_packages(exclude=["data_diff_demo_tests"]),
    install_requires=[
        "dagster==1.5.4",
        "dagster-cloud==1.5.4",
        "data-diff==0.9.7",
        "duckdb==0.9.1",
        "pandas==2.1.1",
        "snowflake-connector-python==3.3.1",
        "psycopg2==2.9.9",
    ],
    extras_require={
        "dev": [
            "dagster-webserver==1.5.4",
            "pytest==7.4.2",
            "ruff==0.1.2",
            "pre-commit==3.5.0",
            "python-dotenv==1.0.0",
        ]
    },
)
