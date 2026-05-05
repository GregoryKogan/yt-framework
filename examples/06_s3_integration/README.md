# S3 listing example

Driver stage builds `S3Client` from `configs/secrets.env`, lists a bucket/prefix, and writes keys into a Cypress table for follow-up work.

## Prereqs

Create `configs/secrets.env`:

```bash
S3_ENDPOINT=https://your-s3-endpoint.example
S3_DOWNLOAD_ACCESS_KEY=...
S3_DOWNLOAD_SECRET_KEY=...
S3_UPLOAD_ACCESS_KEY=...
S3_UPLOAD_SECRET_KEY=...
```

Edit `stages/list_s3/config.yaml` with your bucket and optional prefix/extension limits.

## Run

```bash
python pipeline.py
```

## Files

- `stages/list_s3/stage.py` — list + `save_s3_paths_to_table`
- `configs/secrets.example.env` — template only (copy to `secrets.env`, do not commit real keys)

## Next

- [04_map_operation](../04_map_operation/) to consume rows downstream
- [Secrets](../../docs/configuration/secrets.md)
