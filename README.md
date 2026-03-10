# mkpipe-extractor-elasticsearch

Elasticsearch extractor plugin for [MkPipe](https://github.com/mkpipe-etl/mkpipe). Reads Elasticsearch indices using `elasticsearch-py` scroll API and converts to Spark DataFrames.

## Documentation

For more detailed documentation, please visit the [GitHub repository](https://github.com/mkpipe-etl/mkpipe).

## License

This project is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE) file for details.

---

## Connection Configuration

```yaml
connections:
  es_source:
    variant: elasticsearch
    host: localhost
    port: 9200
    user: elastic
    password: mypassword
```

With API key authentication:

```yaml
connections:
  es_source:
    variant: elasticsearch
    host: localhost
    port: 9200
    api_key: 'your-api-key'
    extra:
      scheme: https
      verify_certs: false
```

---

## Table Configuration

```yaml
pipelines:
  - name: es_to_pg
    source: es_source
    destination: pg_target
    tables:
      - name: my_index
        target_name: stg_my_index
        replication_method: full
        fetchsize: 10000
```

### Incremental Replication

```yaml
      - name: my_index
        target_name: stg_my_index
        replication_method: incremental
        iterate_column: updated_at
        fetchsize: 5000
```

---

## Read Parallelism (Sliced Scroll)

By default, a single scroll session reads all documents sequentially. Setting `partitions_count > 1` enables **Elasticsearch Sliced Scroll**, which splits the index into N independent slices read in parallel using a thread pool:

```yaml
      - name: my_index
        target_name: stg_my_index
        replication_method: full
        partitions_count: 4     # open 4 parallel scroll slices
        fetchsize: 5000
```

### How it works

- Each slice is an independent scroll session with `slice.id` and `slice.max` set
- All slices run concurrently in a `ThreadPoolExecutor` with `partitions_count` workers
- Results are merged into a single Spark DataFrame
- Sliced Scroll is available in Elasticsearch 5.0+

### Performance Notes

- **Small indices (<1M docs):** single scroll is usually sufficient.
- **Large indices (>5M docs):** `partitions_count: 4–8` gives meaningful speed-up.
- Each slice opens its own scroll context — Elasticsearch `max_open_scroll_context` cluster setting applies (default: 500).
- Setting `partitions_count` too high (>16) can stress the ES cluster with concurrent scroll contexts.

---

## All Table Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Elasticsearch index name |
| `target_name` | string | required | Destination table name |
| `replication_method` | `full` / `incremental` | `full` | Replication strategy |
| `iterate_column` | string | — | Field used for incremental range filter |
| `partitions_count` | int | `1` | Number of parallel scroll slices |
| `fetchsize` | int | `10000` | Documents per scroll batch |
| `tags` | list | `[]` | Tags for selective pipeline execution |
| `pass_on_error` | bool | `false` | Skip table on error instead of failing |
