#!/bin/bash

killall -9 multi-producer.app

./multi-producer.app proxy &
./multi-producer.app ponger &> op1 &
./multi-producer.app pinger2 &> op2 &
./multi-producer.app pinger3 &> op3

killall -9 multi-producer.app
