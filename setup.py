from setuptools import setup, find_packages

setup(
    name='shertool',
    version='0.1.0',
    description='none',
    author='shiertier',
    author_email='none',
    url='https://github.com/shiertier/shertool',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    install_requires=[
        line.strip() for line in open('requirements.txt').readlines()
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
)
