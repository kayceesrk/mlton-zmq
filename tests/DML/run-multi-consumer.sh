#!/bin/bash

killall -9 multi-consumer.app

./multi-consumer.app proxy $1 &
./multi-consumer.app pinger $1 &> op1 &
./multi-consumer.app ponger2 $1 &> op2 &
./multi-consumer.app ponger3 $1 &> op3

killall -9 multi-consumer.app
