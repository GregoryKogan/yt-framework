# Build for linux/amd64 platform (required for YT cluster compatibility)
# Use: docker buildx build --platform linux/amd64 --tag <image> --load .
FROM python:3.11-slim

RUN apt-get update && apt-get install -y cowsay && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir omegaconf botocore boto3

RUN ln -sf /usr/games/cowsay /usr/local/bin/cowsay

WORKDIR /app
