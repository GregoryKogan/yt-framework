# Multi-stage pipeline

Three stages run in order: build two small tables, then YQL-join them. Shows how `enabled_stages` orders work and how later stages read Cypress paths produced earlier.

## Run

```bash
python pipeline.py
```

Order: `create_users` → `create_orders` → `join_data`.

## Layout

- `stages/create_users/`, `stages/create_orders/` — seed tables
- `stages/join_data/` — YQL join stage
- `configs/config.yaml` — `enabled_stages` list

## Next

- [03_yql_operations](../03_yql_operations/)
- [04_map_operation](../04_map_operation/)
