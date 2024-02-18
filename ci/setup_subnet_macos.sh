#!/bin/bash
# This script configures both IPv4 and IPv6 loopback addresses.

action=${1:-up}

# Function to configure IPv4 addresses
configure_ipv4() {
    for j in 0 1 2; do
        for ((i=2; i<256; i++)); do
            if [ "$action" = "up" ]; then
                sudo ifconfig lo0 alias 127.0.$j.$i up
            else
                sudo ifconfig lo0 -alias 127.0.$j.$i
            fi
        done
    done
}

# Function to configure IPv6 addresses
configure_ipv6() {
    for ((i=2; i<256; i++)); do
        if [ "$action" = "up" ]; then
            sudo ifconfig lo0 inet6 alias ::1:$i
        else
            sudo ifconfig lo0 inet6 -alias ::1:$i
        fi
    done
}

# Check if loopback for IPv4 is setup
if [ "$action" = "up" ]; then
    ping -c 1 -W 10 127.0.0.2 > /dev/null 2>&1
    if [ $? -eq 0 ]; then
        echo "IPv4 loopback address is already set up."
    else
        configure_ipv4
    fi

    # Check if loopback for IPv6 is setup
    ping6 -c 1 -W 10 ::1:2 > /dev/null 2>&1
    if [ $? -eq 0 ]; then
        echo "IPv6 loopback address is already set up."
    else
        configure_ipv6
    fi
else
    configure_ipv4
    configure_ipv6
fi
