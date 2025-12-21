#!/bin/bash
for tar in ./*.tar; do
    docker load -i "$tar"
done

mkdir -p ./secrets

if [ ! -f "./secrets/key.txt" ]
    read -p "Enter TBA key: " API_KEY
    API_KEY=$(echo "$API_KEY" | xargs) # trim
    echo "$API_KEY" > ./secrets/key.txt
fi

docker-compose up -d