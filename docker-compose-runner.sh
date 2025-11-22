#!/bin/bash
for tar in ./*.tar; do
    docker load -i "$tar"
done

docker compose up -d
