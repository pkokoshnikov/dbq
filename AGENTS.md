# Repository Guidelines

## Project Structure & Module Organization

This repository is a multi-module Maven project for DBQ, a database-backed queue.

- `core/`: domain model, DBQ API, retry/blocking policies, Quartz scheduling, and unit tests in `core/src/test/java`.
- `pg/`: PostgreSQL query implementation and JSONB conversion.
- `spring/`: Spring JDBC and transaction adapters.
- `integration-tests/`: end-to-end tests with Testcontainers and PostgreSQL.
- Root files: `pom.xml` (parent build), `README.MD` (usage), `issues.md` (tracked problems).

Java sources live under `src/main/java/org/pak/messagebus/...`. Test resources use `src/test/resources`.

## Build, Test, and Development Commands

- `mvn -q -pl core test`: run unit tests for `core`.
- `mvn -q -pl integration-tests -am test -DskipTests`: compile all modules through integration-tests without executing Docker-based tests.
- `mvn -q -pl integration-tests -am test`: run the full reactor, including integration tests. Requires Docker/Testcontainers.
- `mvn -q clean verify`: full clean build for all modules.

Use `-pl <module> -am` when changing one module and its dependencies.

## Coding Style & Naming Conventions

- Java 25, Maven build, Lombok annotation processing enabled in the parent `pom.xml`.
- Use 4-space indentation and keep formatting consistent with existing files.
- Packages stay lowercase under `org.pak.messagebus`.
- Class names use `UpperCamelCase`; methods and fields use `lowerCamelCase`.
- Value objects such as `QueueName`, `SubscriptionId`, and `SchemaName` follow explicit naming wrappers rather than raw strings.
- No formatter or lint plugin is configured; keep changes minimal and consistent with the surrounding code.

## Testing Guidelines

- Unit tests use JUnit Jupiter and AssertJ.
- Integration tests live in `integration-tests/src/test/java` and typically extend `BaseIntegrationTest`.
- Name tests with the `*Test` suffix and prefer descriptive method names such as `publishSubscribeTest` or `testHandleRetryableException`.
- For database behavior, add or update integration tests rather than relying only on unit coverage.

## Commit & Pull Request Guidelines

- Recent history uses short imperative subjects, for example: `Fix README.MD`, `Add drop partition test`.
- Keep commit titles concise: `Fix retry delay caching`, `Update Spring and Lombok`.
- PRs should include:
  - a short summary of behavior changes,
  - affected modules,
  - commands run for verification,
  - notes about schema, partitioning, or dependency changes when relevant.

## Development Notes

- Integration tests depend on Docker availability.
- Avoid introducing new dependency versions directly in child modules; prefer centralizing them in the root [`pom.xml`](/Users/p.l.kokoshnikov/repos/message-bus/pom.xml).
