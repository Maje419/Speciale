#!/bin/bash
echo "Enter the username (or 'quit' to exit):"

while read username; do
    # Check if the user wants to quit
    if [ "$username" == "quit" ]; then
        echo "Exiting..."
        break
    fi

    # Construct the curl request with the provided username
    curl -s -o /dev/null "http://localhost:8080/startChoreography?username=$username"
done