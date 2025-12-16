#!/bin/bash
go run mrsequential.go wc.so pg*.txt
sort mr-out-0 > mr-correct-wc.txt
rm -f mr-out*

echo '***' Starting wc test.

# give the coordinator time to create the sockets.
sleep 1

# start multiple workers.
go run mrworker.go wc.so &
go run mrworker.go wc.so &
go run mrworker.go wc.so &

# wait for the coordinator to exit.

# since workers are required to exit when a job is completely finished,
# and not before, that means the job has finished.
sort mr-out* | grep . > mr-wc-all
if cmp mr-wc-all mr-correct-wc.txt
then
  echo '---' wc test: PASS
else
  echo '---' wc output is not the same as mr-correct-wc.txt
  echo '---' wc test: FAIL
  failed_any=1
fi

# wait for remaining workers and coordinator to exit.
wait