from setuptools import setup

setup(
   name='rrutils',
   version='0.0.1',
   description='A collection of Python helper modules for The Office of the Auditor General of Norway (OAGN).',
   author='Aleksander Eilertsen',
   author_email='ale@rikrevisjonen.no',
   url='https://github.com/Riksrevisjonen/rr-python-utils',
   license='MIT',
   packages=['rrutils'],  
   install_requires=['azure-storage-blob'], 
)