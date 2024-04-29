#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from setuptools import find_packages, setup

MAIN_REQUIREMENTS = ["airbyte-cdk", "paramiko~=3.4.0", "asyncssh~=2.14.2", "setuptools~=3.3.0", "pydoop~=2.0.0", "pytz~=2024.1"]

TEST_REQUIREMENTS = ["pytest~=6.2"]

setup(
    name="destination_orange_hdfs",
    description="Destination implementation for Orange Hdfs.",
    author="Airbyte",
    author_email="contact@airbyte.io",
    packages=find_packages(),
    install_requires=MAIN_REQUIREMENTS,
    package_data={"": ["*.json"]},
    extras_require={
        "tests": TEST_REQUIREMENTS,
    },
)
