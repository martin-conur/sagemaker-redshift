from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name='redshift-utils',
    version='0.2.0',
    author='Martin Contreras',
    author_email='martincontrerasur@gmail.com',
    description='A Python utility library for efficient data operations between Amazon Redshift and S3',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/martincontreras/redshift-utils',
    packages=find_packages(),
    py_modules=['redshift_utils'],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Database",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
    install_requires=[
        "boto3>=1.26.0",
        "botocore>=1.29.0",
        "polars>=0.19.0",
        "sagemaker>=2.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "black>=22.0.0",
            "flake8>=5.0.0",
        ]
    },
)