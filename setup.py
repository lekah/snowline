from setuptools import setup, find_packages

setup(
    name = 'snowline',
    version = '0.1',
    description = 'Automated snowline detection',
    download_url = "https://github.com/lekah/snowline",
    author = 'Leonid Kahle',
    license = "MIT",
    python_requires = '>=3.7',
    packages = find_packages(),
    install_requires = [
        "numpy>=1.19.2",
        "scipy>=1.5.3",
        "matplotlib>=3.3.2",
    ],
)
