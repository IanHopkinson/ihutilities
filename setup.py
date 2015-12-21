from setuptools import setup, find_packages

long_desc = """
Miscellaneous utilities for git, I/O and database interaction
"""
# See https://pypi.python.org/pypi?%3Aaction=list_classifiers


conf = dict(
    name='ihutilities',
    version='1.0.0',
    description="Miscellaneous utilities",
    long_description=long_desc,
    classifiers=[
        "Development Status :: 4 - Beta",
        "Programming Language :: Python",
        "License :: OSI Approved :: MIT License",
    ],
    keywords='',
    author='Ian Hopkinson',
    author_email='ianhopkinson@googlemail.com',
    url='http://www.ianhopkinson.org.uk',
    license='MIT',
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    namespace_packages=[],
    include_package_data=False,
    zip_safe=False,
    install_requires=[],
    tests_require=[],
    entry_points={}
    )

if __name__ == '__main__':
    setup(**conf)