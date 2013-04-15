#!/bin/bash

killall -9 cycle.app

./cycle.app proxy &
./cycle.app program

killall -9 cycle.app
