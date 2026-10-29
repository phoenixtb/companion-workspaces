#!/usr/bin/env bash
# Part 27: build the Apache Iceberg Kafka Connect sink from the signed 1.12.0 source release.
# Iceberg publishes no runtime distribution: Maven Central has the plain iceberg-kafka-connect jars but no
# iceberg-kafka-connect-runtime, and the docs say the ZIP "is created as part of the Iceberg build".
# Builds only the runtime module (no Spark, no Flink), with its own Gradle home, then deletes the sources and the
# Gradle home (about 1 GB) and keeps only the unpacked plugin, which compose.yaml mounts into the Connect worker.
# Needs JDK 17 or 21 (Iceberg's build refuses others): JAVA_HOME=/path/to/jdk21 make iceberg-sink
set -euo pipefail
cd "$(dirname "$0")/../.."

VERSION=1.12.0
SHA512=505b4d92baf9a2b55c3893bfa1b50289a0bf0051109d81f0a92825f406c28be9096be31d5c22ed68a2d7abd39fcfb0888729bb1917216b06ac200ac0fbf11a28
URL=https://archive.apache.org/dist/iceberg/apache-iceberg-${VERSION}/apache-iceberg-${VERSION}.tar.gz
WORK=build/iceberg-sink-work
PLUGIN=build/iceberg-kafka-connect-runtime-${VERSION}

if [ -d "$PLUGIN/lib" ]; then echo "already built: $PLUGIN"; exit 0; fi
JAVA="${JAVA_HOME:?set JAVA_HOME to a JDK 17 or 21}/bin/java"
"$JAVA" -version 2>&1 | head -1 | grep -qE '"(17|21)\.' || { echo "JAVA_HOME must be JDK 17 or 21"; exit 1; }

rm -rf "$WORK" && mkdir -p "$WORK"
curl -fsSL -o "$WORK/src.tar.gz" "$URL"
echo "$SHA512  $WORK/src.tar.gz" | shasum -a 512 -c -
tar xzf "$WORK/src.tar.gz" -C "$WORK"

(
  cd "$WORK/apache-iceberg-${VERSION}"
  export GRADLE_USER_HOME="$PWD/../gradle-home"
  # Empty Spark and Flink version lists leave those modules out of the build.
  ./gradlew --no-daemon -q -DsparkVersions= -DflinkVersions= -DkafkaVersions=3 -x test -x integrationTest \
    :iceberg-kafka-connect:iceberg-kafka-connect-runtime:distZip
)
ZIP=$(ls "$WORK"/apache-iceberg-${VERSION}/kafka-connect/kafka-connect-runtime/build/distributions/iceberg-kafka-connect-runtime-*.zip)
echo "built $(basename "$ZIP"): $(du -h "$ZIP" | cut -f1); work dir $(du -sh "$WORK" | cut -f1)"
rm -rf "$PLUGIN" && mkdir -p build && unzip -q "$ZIP" -d build/
rm -rf "$WORK"
echo "plugin: $PLUGIN ($(ls "$PLUGIN/lib" | wc -l | tr -d ' ') jars, $(du -sh "$PLUGIN" | cut -f1))"
