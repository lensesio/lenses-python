from setuptools import setup, find_packages, Command
from os import path
from io import open
import shutil

__CWD = path.abspath(path.dirname(__file__))
with open(path.join(__CWD, 'README.md'), encoding='utf-8') as fstream:
    long_description = fstream.read()

pkgversion = '4.0.0'


class Clean(Command):
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        for d in [
            'build',
            'dist',
            'lensesio.egg-info',
            'lensesio/__pycache__',
            'lensesio/data/__pycache__',
            'lensesio/flows/__pycache__',
            'lensesio/kafka/__pycache__',
            'lensesio/core/__pycache__',
            'lensesio/registry/__pycache__',
            '.tox',
            'venv',
            '.pytest_cache',
        ]:
            try:
                if path.exists(d):
                    shutil.rmtree(d)
                    print("Deleted %s" % d)
            except OSError:
                print("Error while trying to delete %s" % d)


setup(
    name='lensesio',
    version=pkgversion,
    description='Lenses Python Client',
    long_descripion=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/Landoop/lenses-python',
    author='Lenses.io LTD',
    author_email='info@lenses.io',
    license='Apache License 2.0',
    classifiers=[
        'Intended Audience :: System Administrators',
        'Topic :: Software Development',

        'License :: OSI Approved :: Apache Software License',

        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    keywords='lensesio kafka_integration',
    project_urls={
        'Documentation': 'https://docs.lenses.io/2.3/dev/python-lib/',
        'HomePage': 'https://github.com/lensesio/lenses-python',
    },
    package_dir={'': '.'},
    py_modules=['kafka'],
    packages=find_packages(
        include=[
            'lensesio',
            'lensesio.core',
            'lensesio.kafka',
            'lensesio.flows',
            'lensesio.registry',
            'lensesio.data',
            'lensesio.pulsar'
        ],
        exclude=[]
    ),
    python_requires='>=3',
    install_requires=[
        'requests==2.22.0',
        'websocket-client==0.56.0',
    ],
    extras_require={
        'kerberos': [
            'kerberos==1.3.0',
        ],
        'pulsar': [
            'pulsar-client==2.5.1',
        ],
        'full': [
            'pulsar-client==2.5.1',
            'kerberos==1.3.0',
        ],
    },
    cmdclass={
        'clean': Clean,
    },
)
