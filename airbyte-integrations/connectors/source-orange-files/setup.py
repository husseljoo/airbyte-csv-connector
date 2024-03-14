#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from setuptools import find_packages, setup

MAIN_REQUIREMENTS = ["airbyte-cdk", "paramiko~=3.4.0"]

TEST_REQUIREMENTS = ["pytest~=6.2"]

setup(
    name="source_hdfs",
    description="Source implementation for Orange Files.",
    author="Husseljo",
    author_email="contact@airbyte.io",
    packages=find_packages(),
    install_requires=MAIN_REQUIREMENTS,
    package_data={"": ["*.json"]},
    extras_require={
        "tests": TEST_REQUIREMENTS,
    },
)
