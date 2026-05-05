# Custom Docker image

Vanilla job runs inside an image you build (here: `python:3.11-slim` plus `cowsay`). Shows `resources.docker_image` wiring for prod.

## Build (linux/amd64)

```bash
docker buildx build --platform linux/amd64 --tag my-registry/my-image:latest --load .
```

Point `stages/run_in_docker/config.yaml` → `client.operations.vanilla.resources.docker_image` at that tag.

## Run

`configs/config.yaml` uses **prod** (cluster executes the job):

```bash
python pipeline.py
```

## Files

- `Dockerfile` — image recipe
- `stages/run_in_docker/` — vanilla op + `src/vanilla.py` using tools from the image

## Notes

- YT workers expect **linux/amd64** images unless your cell documents otherwise.
- Prod credentials and `build_folder` must be valid for your environment.

## Next

- [Docker](../../docs/advanced/docker.md)
- [video_gpu](../video_gpu/) for GPU base images
