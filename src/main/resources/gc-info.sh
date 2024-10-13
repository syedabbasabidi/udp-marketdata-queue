#!/bin/bash

# Check if at least one PID is passed as an argument
if [ "$#" -lt 1 ]; then
    echo "Usage: $0 <pid1> <pid2> ... <pidN>"
    exit 1
fi

# Define the interval in seconds (5 seconds)
interval=5  # 5-second interval

# Loop through each PID provided as an argument
for pid in "$@"
do

    # Check if the PID is a running Java process
    if ps -p $pid > /dev/null; then
        # Extract and print the header once
        header=$(jstat -gcutil $pid 0 1 | head -n 1)
        echo "$pid - $header"

        # Initialize counter to print the header every 5 rows
        counter=1

        # Infinite loop to run jstat -gcutil forever for this PID, run it in background
        while true; do
            # Run jstat with 1 sample, capture stats every 5 seconds
            jstat -gcutil $pid 0 1 | tail -n +2 | awk -v pid="$pid" '{print "" pid " -", $0}'

            # Print the header every 5 iterations
            if [ $((counter % 5)) -eq 0 ]; then
                echo "$pid - $header"
            fi

            # Increment the counter
            counter=$((counter + 1))

            # Sleep for the interval (5 seconds)
            sleep $interval
        done &
    else
        echo "PID $pid is not a valid running Java process."
    fi

    echo # Print a blank line between PID outputs
done

# Wait for all background processes to complete (which won't happen since it's infinite)
wait

