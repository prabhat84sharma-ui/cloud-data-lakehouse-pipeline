"""Setup script for Cloud Data Lakehouse Pipeline package."""

from setuptools import find_packages, setup

with open("README.md", encoding="utf-8") as f:
    long_description = f.read()

with open("requirements.txt", encoding="utf-8") as f:
    requirements = [
        line.strip()
        for line in f
        if line.strip() and not line.startswith("#") and not line.startswith("-")
    ]

setup(
    name="cloud-data-lakehouse-pipeline",
    version="1.0.0",
    author="Prabhat Sharma",
    author_email="prabhat84sharma@users.noreply.github.com",
    description="End-to-End Medallion Lakehouse Pipeline with PySpark, Delta Lake, and S3",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/prabhat84sharma-ui/cloud-data-lakehouse-pipeline",
    packages=find_packages(where=".", include=["src*"]),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.10",
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "lakehouse-pipeline=src.pipeline:main",
        ],
    },
)
