from setuptools import setup, find_packages

setup(
    name='phxfs',
    version='0.1.2',
    packages=find_packages(),
    author='kuangkai',
    author_email='kuangkai@kylinos.cn',
    description='The python module of phoenix api',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/nicexlab/phoenix',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.7',
)

