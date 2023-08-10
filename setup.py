from setuptools import setup, find_packages

with open("requirements.txt") as install_requires_file:
    requirements = install_requires_file.read().strip().split("\n")

setup(
    name="core",
    description="Scripts for flows",
    #@license="Apache License 2.0",
    #author="",
    #author_email="",
    #keywords="",
    packages=["core"],
    package_dir={"core": "src/core"},
    version="1.0",
    python_requires=">=3.8",
    install_requires=requirements
)