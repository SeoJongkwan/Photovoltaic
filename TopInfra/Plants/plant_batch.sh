#!/bin/bash
plants=("plant1" "plant2" "plant4" "plant13" "plant15" "plant16" "plant23" "plant28" "plant29" "plant63")
# plants='plant1'

for plant in "${plants[@]}"
do
    echo "Select Plant: "$plant
    python3 /home/p2g/bellk/topinfra/Plants/top_client.py $plant
    # docker run -d --network top_net -p 2201:2255 --name top_$plant top_client $plant
done