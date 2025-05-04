#!/bin/bash

if [ -f "$0" ]; then script=$0; else script=$(command -v -- "$0"); fi
dir=$(dirname -- "$script")

echo "Writing data to $dir"

wget -O "$dir/reviews.csv.gz" https://data.insideairbnb.com/united-kingdom/england/london/2024-09-06/data/reviews.csv.gz
wget -O "$dir/calendar.csv.gz" https://data.insideairbnb.com/united-kingdom/england/london/2024-09-06/data/calendar.csv.gz
wget -O "$dir/listings.csv.gz" https://data.insideairbnb.com/united-kingdom/england/london/2024-09-06/data/listings.csv.gz
