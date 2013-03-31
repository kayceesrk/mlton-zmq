#!/bin/bash

killall -9 multi-consumer.app

./multi-consumer.app proxy &
./multi-consumer.app pinger &> op1 &
./multi-consumer.app ponger2 &> op2 &
./multi-consumer.app ponger3 &> op3

killall -9 multi-consumer.app
