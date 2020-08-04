import setuptools

with open('README.md') as f:
    long_description = f.read()

with open('requirements.txt') as f:
    requirements = f.readlines()

setuptools.setup(
    name='py-turbo',
    version='0.2.8',
    author='Lijun Yu',
    author_email='lijun@lj-y.com',
    description='A pipeline system for efficient execution.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/CMU-INF-DIVA/pyturbo',
    license='GPL',
    packages=setuptools.find_packages(),
    install_requires=requirements,
    python_requires='>=3.6',
)
