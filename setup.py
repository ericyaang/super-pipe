from setuptools import setup, find_packages

with open("requirements.txt", "r", encoding="utf-8") as install_requires_file:
    requirements = install_requires_file.read().strip().split("\n")

setup(
    name="core",
    description="Scripts for flows",
    #@license="Apache License 2.0",
    #author="",
    #author_email="",
    #keywords="",
    packages=find_packages(where="src"),
    package_dir={"core": "src/core"},
    version="1.0",
    python_requires=">=3.10",
    install_requires=requirements
)