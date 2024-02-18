#!/bin/bash

#
# This script makes sure that 127.0.0.x and ::1:x are routable. On some systems,
# there might be a need to explicitly add these addresses to the loopback interface.
#

action=${1:-up}

# IPv4 Setup or Removal
if [ "$action" = "up" ]; then
  # Check if IPv4 loopback is setup
  ip addr show lo | grep -q "127.0.0.2"
  if [ $? -eq 0 ]; then
      echo "IPv4 loopback address already set up."
  else
    for j in 0 1 2; do
      for i in {2..255}; do
          sudo ip addr add 127.0.$j.$i/8 dev lo
      done
    done
  fi
else
  for j in 0 1 2; do
    for i in {2..255}; do
        sudo ip addr del 127.0.$j.$i/8 dev lo
    done
  done
fi

# IPv6 Setup or Removal
if [ "$action" = "up" ]; then
  # Check if IPv6 loopback is setup
  ip -6 addr show lo | grep -q "::1:2"
  if [ $? -eq 0 ]; then
      echo "IPv6 loopback address already set up."
  else
    for i in {2..255}; do
      sudo ip -6 addr add fc00::1:$i/128 dev lo
    done
  fi
else
  for i in {2..255}; do
    sudo ip -6 addr del fc00::1:$i/128 dev lo
  done
fi
