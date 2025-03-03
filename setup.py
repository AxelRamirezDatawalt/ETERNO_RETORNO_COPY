from setuptools import find_packages, setup

setup(
    name="eterno_retorno",
    packages=find_packages(exclude=["eterno_retorno_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "pandas",
        "pyodbc",
        'dagster-gcp',
        "dagster-docker",
        "dagster-postgres",
        "dagster-embedded-elt",
        "sling",
        "dagster-gcp-pandas",
  
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
