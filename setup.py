from setuptools import setup

setup(
    name='lenses_python',
    version='2.0.1',
    packages=['lenses_python'],
    url='https://github.com/Landoop/lenses-python',
    download_url='https://github.com/Landoop/lenses-python/archive/2.0.tar.gz',
    license='Apache License 2.0',
    author='Filippas Serepas',
    author_email='info@landoop.com',
    description='lenses python client',
    python_requires='>=3',
    install_requires=['requests', 'pandas==0.22.0', 'websocket-client==0.47.0','sseclient-py==1.7']


)
