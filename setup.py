#!/usr/bin/env python3
import os, sys
import setuptools

# Get text from README.txt
with open("README.md", "r") as fp:
    readme_text = fp.read()

# Get __version__ without importing
with open(os.path.join(os.path.dirname(__file__),"ndsb", "__init__.py"), "r") as f:
    for line in f:
        if line.startswith("__version__ = "):
            exec(line.strip())
            break

setuptools.setup(
    name="ndsb",
    version=__version__,
    description="Collect data, turn it into static artifacts and beam it to a vault.",
    license="MIT",
    author="Robin De Schepper",
    author_email="robingilbert.deschepper@unipv.it",
    url="https://github.com/dbbs-lab/ndsb",
    long_description=readme_text,
    long_description_content_type="text/markdown",
    packages=["ndsb"],
    classifiers=[
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
    ],
    install_requires=["portalocker"],
    extras_require={"dev": ["sphinx", "sphinx_rtd_theme>=0.4.3", "pre-commit", "black"],},
)
