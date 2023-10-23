from setuptools import find_packages, setup

setup(
    name="data_diff_demo",
    packages=find_packages(exclude=["data_diff_demo_tests"]),
    install_requires=[
        "dagster==1.5.4",
        "dagster-cloud==1.5.4",
        "data-diff==0.9.7",
        "duckdb==0.9.1"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
