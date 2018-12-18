from setuptools import setup

setup(name='bruny-island-forecast-downloader',
      version='1.0',
      description='downloads and stores forecasts for bruny island research weather sites to csv',
      url='https://github.com/jmccorkindale/bruny-island-research',
      author='Jared McCorkindale',
      author_email='jared.mccorkindale@anu.edu.au',
      packages=['bruny-island-research'],
      install_requires=['requests'],
      zip_safe=False)
