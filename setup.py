from setuptools import setup, find_packages

setup(
  name='pipette',
  version='1.0.0',
  description='Python framework for managing experiment workflows',
  url='https://github.com/allenai/pipette',
  packages=find_packages(),
  py_modules=['pipette'],
  test_suite="pipette",
  install_requires=[
    'dill',
    'mmh3',
    'datastore @ https://github.com/allenai/datastore/releases/download/v3.0.0/datastore-3.0.0-py3-none-any.whl',
    'manhole',
    'typeguard'
  ],
  python_requires='>=3.6'
)
