# Companion Workspaces

Runnable companion code for the tutorial series published on [pub.titasbiswas.com](https://pub.titasbiswas.com).

Each folder is a **self-contained workspace for one series**: its own build, wrapper, versions, README and infrastructure. There is no root build, and no folder depends on another. Open the folder for the series you are reading, not the repository root.

| Series | Folder | Blog |
| --- | --- | --- |
| Kotlin for Java Survivors: Life After Semicolons | [`kotlin-for-java-survivors/`](kotlin-for-java-survivors/) | [pub.titasbiswas.com/series/kotlin-for-java-survivors](https://pub.titasbiswas.com/series/kotlin-for-java-survivors/) |
| The Log Never Lies: Change Data Capture with Debezium | [`cdc-with-debezium/`](cdc-with-debezium/) | [pub.titasbiswas.com/series/cdc-with-debezium](https://pub.titasbiswas.com/series/cdc-with-debezium/) |
| Every Record Counts: Data Engineering for Backend Engineers | [`every-record-counts/`](every-record-counts/) | [pub.titasbiswas.com/series/every-record-counts](https://pub.titasbiswas.com/series/every-record-counts/) |

## Conventions

- Tags are namespaced by series, for example `kotlin-for-java-survivors/part-13`, `cdc-with-debezium/part-1` or `every-record-counts/part-0`. Check out a tag to see the code exactly as a part left it.
- Every code block in a post exists in the matching folder and has been run. Printed output in the posts comes from real runs; each folder's README says which outputs are also asserted by tests.

## Licence

[MIT](LICENSE)
