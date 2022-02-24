#!/usr/bin/env python
# coding: utf8

from setuptools import setup

with open('README.md') as f:
    readme = f.read()

setup(
    name='toloka-prefect',
    packages=['toloka_prefect'],
    version='0.0.3',
    description='Toloka prefect tasks library',
    long_description=readme,
    long_description_content_type='text/markdown',
    license='Apache 2.0',
    author='Vladislav Moiseev',
    author_email='vlad-mois@yandex-team.ru',
    python_requires='>=3.7.0',
    install_requires=[
        'pandas >= 1.1.0',
        'prefect',
        'requests',
        'toloka-kit==0.1.23',
    ],
    include_package_data=True,
    project_urls={
        'Source': 'https://github.com/Toloka/toloka-prefect',
    },
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: Implementation :: CPython',
        'Topic :: Scientific/Engineering',
        'Topic :: Software Development',
        'Typing :: Typed',
    ],
)
