from setuptools import setup

from pysql_beam_lib import __version__

setup(
    name='pysql_beam_lib',
    version=__version__,

    url='https://github.com/aandis97/pysql_beam_lib',
    author='Achmad Andi Setuawan',
    author_email='achandis97@gmail.com',

    py_modules=['pysql_beam_lib'],
)