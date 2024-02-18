#!/bin/bash

# This script configures additional IPv4 and IPv6 addresses on the loopback interface on macOS.
# Usage: ./script.sh [up|down]

action=${1:-up}

configure_ipv4() {
  for j in {0..2}; do
    for i in {2..255}; do
      if [ "$action" = "up" ]; then
        sudo ifconfig lo0 alias 127.0.$j.$i up
      else
        sudo ifconfig lo0 -alias 127.0.$j.$i
      fi
    done
  done
}

configure_ipv6() {
  # Example uses ULA (Unique Local Addresses) fc00::/7 for IPv6 to avoid potential conflicts.
  # Adjust the IPv6 range as needed for your specific use case.
  for i in {2..255}; do
    if [ "$action" = "up" ]; then
      sudo ifconfig lo0 inet6 alias fc00::1:$i/128
    else
      sudo ifconfig lo0 inet6 -alias fc00::1:$i
    fi
  done
}

# Check if the first additional IPv4 loopback address is set up
if ifconfig lo0 | grep -q "127.0.0.2"; then
  echo "IPv4 additional loopback address detected."
else
  echo "Setting up IPv4 additional loopback addresses."
  configure_ipv4
fi

# Check if the first additional IPv6 loopback address is set up
if ifconfig lo0 | grep -q "fc00::1:2"; then
  echo "IPv6 additional loopback address detected."
else
  echo "Setting up IPv6 additional loopback addresses."
  configure_ipv6
fi

# Remove addresses if the action is 'down'
if [ "$action" = "down" ]; then
  echo "Removing configured IPv4 and IPv6 loopback addresses."
  configure_ipv4
  configure_ipv6
fi
