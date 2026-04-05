set -e

INPUT_PATH=${1:-/input/data}
STREAMING_JAR=$(ls /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming*.jar | head -n 1)

echo "Using input: ${INPUT_PATH}"
echo "Using streaming jar: ${STREAMING_JAR}"

hdfs dfs -rm -r -f /indexer/index || true
hdfs dfs -rm -r -f /indexer/vocab || true

hadoop jar "${STREAMING_JAR}" \
  -D mapreduce.job.name="assignment2-index-pipeline1" \
  -D mapreduce.job.reduces=1 \
  -files /app/mapreduce/mapper1.py,/app/mapreduce/reducer1.py \
  -mapper "python3 mapper1.py" \
  -reducer "python3 reducer1.py" \
  -input "${INPUT_PATH}" \
  -output /indexer/index

hadoop jar "${STREAMING_JAR}" \
  -D mapreduce.job.name="assignment2-vocab-pipeline2" \
  -D mapreduce.job.reduces=1 \
  -files /app/mapreduce/mapper2.py,/app/mapreduce/reducer2.py \
  -mapper "python3 mapper2.py" \
  -reducer "python3 reducer2.py" \
  -input /indexer/index \
  -output /indexer/vocab

echo "Done."
hdfs dfs -ls /indexer
