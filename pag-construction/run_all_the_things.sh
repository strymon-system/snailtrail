#!/bin/bash
set -e
OUTDIR="${OUTDIR:-results/}"

#MODE="plot"
MODE_SUMMARY="${MODE:-summary}"
MODE_CUSTOM="${MODE:-custom}"
MODE_LATENCY="${MODE:-latency}"
MODE_THROUGHPUT="${MODE:-throughput}"
MODE_SCALABILITY="${MODE_THROUGHPUT}"

SUMMARY_PARAMS="--min-window 1 --max-window 32 \
--min-workers 8 --max-workers 8 \
--min-epochs 16 --max-epochs 16 \
--min-threshold 1000000000 --max-threshold 1000000000"

TEST_PARAMS="--min-window 1 --max-window 4 \
--min-workers 8 --max-workers 8 \
--min-epochs 16 --max-epochs 16 \
--min-threshold 1000 --max-threshold 1000"

MD="--message-delay 100000"

# .1s
MW="--waiting-message 100000000"

HERON_PARAMS="--no-insert-waiting true"

msgpack_spark_list=(
)

for msgpack in "${msgpack_spark_list[@]}"
do
  ./run_benchmark.py $MODE_SUMMARY "${msgpack}" --outdir $OUTDIR      $SUMMARY_PARAMS || exit
  ./run_benchmark.py $MODE_SUMMARY "${msgpack}" --outdir "$OUTDIR/md" $SUMMARY_PARAMS $MD || exit
done


HERON=""
#./run_benchmark.py $MODE_CUSTOM  --name heron_experiment --input $HERON/snailtrail.logrecords.metricsmgr-1.0 --outdir "${OUTDIR}/mw_off" $SUMMARY_PARAMS $HERON_PARAMS --config config_heron || exit
#./run_benchmark.py $MODE_SUMMARY --name heron_experiment --input $HERON/snailtrail.logrecords.metricsmgr-1.0 --outdir "${OUTDIR}/mw_off" $SUMMARY_PARAMS $HERON_PARAMS --config config_heron || exit

# Run tests

if [ "$MODE" = "run" ]; then
  python ./run_test.py
fi

./run_benchmark.py $MODE_SUMMARY --name test --input test/*.msgpack --outdir "${OUTDIR}/" $TEST_PARAMS || exit
