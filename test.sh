#!/bin/bash

# Configuration
VEHICLE_ID=$1         # Example vehicle ID
GRPC_SERVER="localhost:800${VEHICLE_ID}"  # Adjust the server address as needed

while true; do
    # Current timestamp for updating some dynamic values
    TIMESTAMP=$(date +%s)

    # Create JSON payload for the vehicle update
    JSON_PAYLOAD=$(cat <<EOF
{
  "vehicle": {
    "id": "${VEHICLE_ID}",
    "x": $(echo "scale=6; 37.7749 + $(($RANDOM % 100 - 50)) / 10000" | bc),
    "y": $(echo "scale=6; -122.4194 + $(($RANDOM % 100 - 50)) / 10000" | bc),
    "speed": $(( $RANDOM % 30 + 50 )),
    "heading": $(( $RANDOM % 360 ))
  }
}
EOF
)

    # Make the gRPC call
    echo "Updating vehicle ${VEHICLE_ID}..."
    grpcurl -d "${JSON_PAYLOAD}" \
        -plaintext \
        ${GRPC_SERVER} \
        app.PlatoonService/UpdateVehicle

    # Wait for 500ms
    sleep 0.5
done
