from setuptools import setup, find_packages

setup(
    name='mkpipe-extractor-elasticsearch',
    version='0.1.1',
    license='Apache License 2.0',
    packages=find_packages(),
    install_requires=['mkpipe', 'elasticsearch>=8.0'],
    include_package_data=True,
    entry_points={
        'mkpipe.extractors': [
            'elasticsearch = mkpipe_extractor_elasticsearch:ElasticsearchExtractor',
        ],
    },
    description='Elasticsearch extractor for mkpipe.',
    author='Metin Karakus',
    author_email='metin_karakus@yahoo.com',
    python_requires='>=3.9',
)
