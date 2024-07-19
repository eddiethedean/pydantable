import pathlib
from setuptools import setup, find_packages

HERE = pathlib.Path(__file__).parent

README = (HERE / "README.md").read_text()

setup(
    name="pydantable",
    version="0.0.1",
    description="Python package that uses pydantic to store data in a data table.",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/eddiethedean/pydantable",
    author="Odos Matthews",
    author_email="odosmatthews@gmail.com",
    license="MIT",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
    ],
    packages=find_packages(),
    include_package_data=True,
    python_requires='>=3.8',
    install_requires=['tinytable', 'pydantic', 'tinytim']
)