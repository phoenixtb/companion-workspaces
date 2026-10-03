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

Act III needs Docker. The first start of the AI service downloads the Ollama models (about 2.8 GB).

```bash
docker compose -f infra/compose.yaml up -d postgres kafka qdrant ollama
./gradlew :services:product-catalog-service:bootRun                              # port 8080
./gradlew :services:product-ai-service:bootRun                                   # port 8081, Spring AI adapter
SPRING_PROFILES_ACTIVE=lc4j ./gradlew :services:product-ai-service:bootRun       # LangChain4j adapter
curl -s -X POST localhost:8080/api/products/republish                            # index the seed catalog
```

## Parts

| Part | Post | Code |
| --- | --- | --- |
| 0 | [Kotlin for Java Survivors: Life After Semicolons](https://pub.titasbiswas.com/series/kotlin-for-java-survivors/kotlin-for-java-survivors-part-0-overview/) | — |
| 1 | [Life After Semicolons](https://pub.titasbiswas.com/series/kotlin-for-java-survivors/kotlin-for-java-survivors-part-1-life-after-semicolons/) | `language/part01-life-after-semicolons` |
| 2 | [The Billion-Dollar Fix](https://pub.titasbiswas.com/series/kotlin-for-java-survivors/kotlin-for-java-survivors-part-2-the-billion-dollar-fix/) | `language/part02-the-billion-dollar-fix` |
| 3 | [Classes Without Ceremony](https://pub.titasbiswas.com/series/kotlin-for-java-survivors/kotlin-for-java-survivors-part-3-classes-without-ceremony/) | `language/part03-classes-without-ceremony` |
| 4 | [Data, Sealed and Value](https://pub.titasbiswas.com/series/kotlin-for-java-survivors/kotlin-for-java-survivors-part-4-data-sealed-and-value/) | `language/part04-data-sealed-and-value` |
| 5 | [Functions Are Values](https://pub.titasbiswas.com/series/kotlin-for-java-survivors/kotlin-for-java-survivors-part-5-functions-are-values/) | `language/part05-functions-are-values` |
| 6 | [Extend, Scope, Delegate](https://pub.titasbiswas.com/series/kotlin-for-java-survivors/kotlin-for-java-survivors-part-6-extend-scope-delegate/) | `language/part06-extend-scope-delegate` |
| 7 | [Collections Without Collectors](https://pub.titasbiswas.com/series/kotlin-for-java-survivors/kotlin-for-java-survivors-part-7-collections-without-collectors/) | `language/part07-collections-without-collectors` |
| 8 | [Generics Without Wildcards](https://pub.titasbiswas.com/series/kotlin-for-java-survivors/kotlin-for-java-survivors-part-8-generics-without-wildcards/) | `language/part08-generics-without-wildcards` |
| 9 | [Errors Without Checked Exceptions](https://pub.titasbiswas.com/series/kotlin-for-java-survivors/kotlin-for-java-survivors-part-9-errors-without-checked-exceptions/) | `language/part09-errors-without-checked-exceptions` |
| 10 | [Suspend Your Disbelief](https://pub.titasbiswas.com/series/kotlin-for-java-survivors/kotlin-for-java-survivors-part-10-suspend-your-disbelief/) | `language/part10-suspend-your-disbelief` |
| 11 | [Flowing Data](https://pub.titasbiswas.com/series/kotlin-for-java-survivors/kotlin-for-java-survivors-part-11-flowing-data/) | `language/part11-flowing-data` |
| 12 | [Bilingual](https://pub.titasbiswas.com/series/kotlin-for-java-survivors/kotlin-for-java-survivors-part-12-bilingual/) | `language/part12-bilingual` |
| 13 | [The Catalog](https://pub.titasbiswas.com/series/kotlin-for-java-survivors/kotlin-for-java-survivors-part-13-the-catalog/) | `services/product-catalog-service` (tag `kotlin-for-java-survivors/part-13`) |
| 14 | [Reactive and Events](https://pub.titasbiswas.com/series/kotlin-for-java-survivors/kotlin-for-java-survivors-part-14-reactive-and-events/) | `services/product-catalog-service`, `shared/product-events` (tag `kotlin-for-java-survivors/part-14`) |
| 15 | [The Assistant](https://pub.titasbiswas.com/series/kotlin-for-java-survivors/kotlin-for-java-survivors-part-15-the-assistant/) | `services/product-ai-service` (tag `kotlin-for-java-survivors/part-15`) |
| 16 | [The Second Opinion](https://pub.titasbiswas.com/series/kotlin-for-java-survivors/kotlin-for-java-survivors-part-16-the-second-opinion/) | `services/product-ai-service`, `lc4j` profile (tag `kotlin-for-java-survivors/part-16`) |

## Layout

- `language/` — Parts 1–12: one module per part; `testkit` runs every example's `main()` and compares its output with the `// ->` comments.
- `services/product-catalog-service` — Spring Boot 4.1, JPA, REST, Kafka producer (Parts 13–14).
- `services/product-ai-service` — Spring AI 2.0 and LangChain4j adapters behind the same ports (Parts 15–16).
- `shared/product-events` — the sealed event hierarchy both services share.
- `build-logic/` — convention plugins; `infra/compose.yaml` — PostgreSQL, Kafka, Qdrant, Ollama.
