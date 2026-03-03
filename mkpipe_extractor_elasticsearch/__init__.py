from typing import Optional

from mkpipe.spark.base import BaseExtractor
from mkpipe.models import ConnectionConfig, ExtractResult, TableConfig
from mkpipe.utils import get_logger

logger = get_logger(__name__)


class ElasticsearchExtractor(BaseExtractor, variant='elasticsearch'):
    def __init__(self, connection: ConnectionConfig):
        self.connection = connection
        self.host = connection.host or 'localhost'
        self.port = connection.port or 9200
        self.username = connection.user
        self.password = connection.password
        self.scheme = connection.extra.get('scheme', 'http')
        self.api_key = connection.api_key

    def extract(self, table: TableConfig, spark, last_point: Optional[str] = None) -> ExtractResult:
        logger.info({
            'table': table.target_name,
            'status': 'extracting',
            'replication_method': table.replication_method.value,
        })

        from elasticsearch import Elasticsearch
        import pandas as pd

        es_kwargs = {
            'hosts': [f'{self.scheme}://{self.host}:{self.port}'],
            'verify_certs': self.connection.extra.get('verify_certs', False),
        }
        if self.api_key:
            es_kwargs['api_key'] = self.api_key
        elif self.username and self.password:
            es_kwargs['basic_auth'] = (self.username, self.password)

        es = Elasticsearch(**es_kwargs)

        index = table.name
        query = {'match_all': {}}

        if table.replication_method.value == 'incremental' and last_point and table.iterate_column:
            query = {'range': {table.iterate_column: {'gt': last_point}}}
            write_mode = 'append'
        else:
            write_mode = 'overwrite'

        scroll_size = table.fetchsize or 10000
        results = []
        resp = es.search(index=index, query=query, scroll='5m', size=scroll_size)
        scroll_id = resp['_scroll_id']
        hits = resp['hits']['hits']

        while hits:
            for hit in hits:
                doc = hit['_source']
                doc['_id'] = hit['_id']
                results.append(doc)
            resp = es.scroll(scroll_id=scroll_id, scroll='5m')
            scroll_id = resp['_scroll_id']
            hits = resp['hits']['hits']

        es.clear_scroll(scroll_id=scroll_id)

        if not results:
            logger.info({'table': table.target_name, 'status': 'extracted', 'rows': 0})
            return ExtractResult(df=None, write_mode=write_mode)

        pdf = pd.DataFrame(results)
        df = spark.createDataFrame(pdf)

        last_point_value = None
        if table.replication_method.value == 'incremental' and table.iterate_column:
            from pyspark.sql import functions as F
            row = df.agg(F.max(table.iterate_column).alias('max_val')).first()
            if row and row['max_val'] is not None:
                last_point_value = str(row['max_val'])

        logger.info({
            'table': table.target_name,
            'status': 'extracted',
            'write_mode': write_mode,
            'rows': len(results),
        })

        return ExtractResult(df=df, write_mode=write_mode, last_point_value=last_point_value)
