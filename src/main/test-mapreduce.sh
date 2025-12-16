#!/bin/bash

go run mrworker.go wc.so &
go run mrworker.go wc.so &
go run mrworker.go wc.so &
go run mrworker.go wc.so &

wait

echo "所有worker已经退出"