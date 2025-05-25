from setuptools import setup, find_packages

setup(
    name='hg_localization',
    version='0.1.0',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'click',
        'datasets',
    ],
    entry_points='''
        [console_scripts]
        hg-localize=hg_localization.cli:cli
    ''',
    author="Your Name", # Replace with your name
    author_email="your.email@example.com", # Replace with your email
    description="A Python library to localize Hugging Face datasets.",
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url="https://github.com/ECNU3D/hg-localization", # Replace with your repo URL
) 