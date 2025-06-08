#!/bin/bash
# shellcheck disable=SC2034
for i in {1..10000}; do
   telnet localhost 9092 &
done
wait