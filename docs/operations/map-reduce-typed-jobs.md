# Map-reduce: TypedJob mapper/reducer

## `typed_reduce_row_iterator_io`

Some documentation suggests aligning map-reduce reducer I/O with standalone reduce. **Many production clusters (including setups where reduce jobs in map-reduce do not support the `row_index` control attribute) will reject the spec** with an error such as:

> `"row_index" control attribute is not supported by "reduce" jobs in map-reduce operation`

Keep `typed_reduce_row_iterator_io: false` (default) unless your cluster explicitly supports it.

## Mapper batch I/O (`RowIterator`)

For **native typed batch input on the map leg**, implement the mapper as:

```python
def __call__(self, rows: RowIterator[InRow]) -> Iterable[OutRow]:
    for row in rows:
        ...
```

This matches reducer-side `RowIterator[...]` batching. Non-TypedJob pipelines can use `ytjobs.mapper.BatchMapper` for JSON stdin/stdout batching instead.

## Operation description

A string `operation_description` in stage config is logged when submitting. If your cluster accepts a structured top-level `description` map, pass a dict via config and the framework forwards it to the spec builder.
