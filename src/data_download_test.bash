#!/bin/bash
mkdir -p data
mkdir -p stream_data

for i in {0..4}
do
  wget "https://data.gharchive.org/2015-01-01-${i}.json.gz" -P data/
done

echo "batch data downloaded!"
