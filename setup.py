from setuptools import setup, find_packages

setup(
  name='pipette',
  version='1.1.0',
  description='Python framework for managing experiment workflows',
  url='https://github.com/allenai/pipette',
  packages=find_packages(),
  py_modules=['pipette'],
  test_suite="tests",
  install_requires=[
    'dill',
    'mmh3',
    'typeguard'
  ],
  python_requires='>=3.6'
)
