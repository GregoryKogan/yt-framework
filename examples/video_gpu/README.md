# GPU map sample

Map stage requests `gpu_limit` and a CUDA-capable `docker_image`. The included `Dockerfile` is a starting point only—pin versions to what your cluster supports.

## Build

```bash
docker buildx build --platform linux/amd64 --tag my-registry/gpu-image:latest --load .
```

Update `stages/run_map/config.yaml` with that tag, keep `pipeline.mode: prod`, valid `build_folder`, and credentials.

## Run

```bash
python pipeline.py
```

## Files

- `stages/run_map/Dockerfile` — CUDA runtime + Python deps
- `stages/run_map/src/mapper.py` — device selection sketch
- Earlier stages create/join tables feeding the map

## Config snippet

```yaml
client:
  operations:
    map:
      resources:
        docker_image: my-registry/gpu-image:latest
        gpu_limit: 1
        memory_limit_gb: 16
        cpu_limit: 4
        pool: default
```

## Next

- [Docker](../../docs/advanced/docker.md)
- [04_map_operation](../04_map_operation/)
