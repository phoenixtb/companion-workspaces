# Kotlin for Java Survivors — Companion Code

Runnable code for the blog series **"Kotlin for Java Survivors: Life After Semicolons"**.

Every Kotlin block in the posts lives here, compiles, and has its printed output checked by a test.

## Prerequisites

| Tool | Version |
| --- | --- |
| JDK | 25 (Gradle downloads one through the foojay resolver if none is installed) |
| Gradle | Use the wrapper (`./gradlew`, Gradle 9.7.1) |
| Docker | Needed only for Act III integration tests and `infra/compose.yaml` |

All versions are pinned once in [`gradle/libs.versions.toml`](gradle/libs.versions.toml).

## Running

```bash
./gradlew build                                   # compile and test everything
./gradlew :language:part01-life-after-semicolons:test
```

## Parts

_The index is filled in as parts are written._
