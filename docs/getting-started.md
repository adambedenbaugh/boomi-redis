# Getting Started

This guide walks you from a fresh clone of the repo to a built connector you can
upload into Boomi — plus how to run the tests. It assumes you can find your way
around a terminal and have written a little Java, but it does **not** assume you
know Gradle, Boomi connectors, or Redis. Every step explains *what* it does and
*why*.

If you just want the reference tables (every connection field, every operation),
read the [main README](../README.md) after you finish here.

---

## 1. What you're building

This repo produces a **Boomi custom connector for Redis** — a plugin that lets a
Boomi integration read from and write to a Redis cache (GET / UPSERT / DELETE).
"Building" the project compiles the Java code into a single uploadable file (a
`.car` archive) plus a descriptor that tells Boomi what fields and operations the
connector has. You then upload those two files into the Boomi platform.

You do **not** need a Redis server or a Boomi account just to build the project or
run the unit tests. You only need those for the later, optional steps.

---

## 2. Prerequisites

Install these first. The **Check** column shows a command you can run in a
terminal to confirm each one is present.

| Tool | Why you need it | Check |
|------|-----------------|-------|
| **Git** | To clone the repo | `git --version` |
| **Java JDK 8 or newer** | The connector is written in Java 8 and compiled by the JDK | `java -version` |
| **Docker Desktop** *(optional)* | Only needed for some integration tests; the build and unit tests don't use it | `docker --version` |
| **A Boomi account** *(optional)* | Only needed to actually upload and use the connector | — |

Notes:

- **You do not need to install Gradle.** The repo ships with the *Gradle wrapper*
  (`gradlew` / `gradlew.bat`) — a small script that downloads the correct Gradle
  version for you the first time you build. That first build will be slower
  because it's downloading Gradle and all the libraries; later builds are fast.
- **JDK vs JRE:** you need the **JDK** (Java Development Kit — it can compile
  code), not just the JRE (which can only run it). If `java -version` prints a
  version but the build later complains about compiling, install a JDK (e.g.
  [Adoptium Temurin 8+](https://adoptium.net/)).
- The code targets **Java 8** on purpose (Boomi runtime requirement). A newer JDK
  (11, 17, 21) is fine to build with — it just compiles *down* to Java 8.

---

## 3. Get the code

```bash
git clone https://github.com/adambedenbaugh/boomi-redis.git
cd boomi-redis
```

Open the folder in your editor (VS Code, IntelliJ, etc.). The parts you'll care
about:

| Path | What's in it |
|------|--------------|
| [src/main/java/com/boomi/connector/redis/](../src/main/java/com/boomi/connector/redis/) | The connector's Java source code |
| [src/main/resources/META-INF/](../src/main/resources/META-INF/) | `connector-descriptor.xml` — defines the fields/operations Boomi shows |
| [src/test/java/](../src/test/java/) | Unit and integration tests |
| [src/test/resources/](../src/test/resources/) | Config templates for integration tests (the `*.example` files) |
| [build.gradle](../build.gradle) | The build recipe (dependencies, tasks, output) |

---

## 4. Build the connector

From the project root, run the wrapper. **On Windows (PowerShell):**

```powershell
.\gradlew.bat build
```

**On macOS / Linux:**

```bash
./gradlew build
```

> Throughout this guide, wherever you see `./gradlew ...`, use `.\gradlew.bat ...`
> on Windows. They do the same thing.

`build` does three things: compiles the code, **runs the unit tests**, and
packages the connector. When it finishes you'll see `BUILD SUCCESSFUL` and two
files you care about in **`build/connector-upload/`**:

- `BoomiRedisConnector-<version>.car` — the connector archive (the current
  version is set in [build.gradle](../build.gradle#L27))
- `connector-descriptor.xml` — the field/operation definitions

Those two files are exactly what you upload to Boomi in step 7.

> **Tip:** if the build fails, scroll up to the *first* red error, not the last —
> Gradle prints a summary at the bottom but the real cause is usually higher up.
> See [Troubleshooting](#9-troubleshooting).

---

## 5. Run the unit tests

Unit tests check the connector's logic in isolation. They use fake/mocked Redis
connections, so **they need no Redis server and no Docker** — they're safe to run
anytime.

```bash
./gradlew test
```

`build` already runs these, so use `test` when you just want a quick check after
editing code. A passing run ends with `BUILD SUCCESSFUL`; failures are listed by
test name, and a full HTML report is written to
`build/reports/tests/test/index.html` (open it in a browser).

---

## 6. Run the integration tests (optional)

Integration tests exercise the connector against a **real Redis**. They're
excluded from the normal `test` run and only execute when you ask for them:

```bash
./gradlew integrationTest
```

There are **two kinds**, and they get their Redis differently:

**a) Testcontainers tests** (e.g. [RedisEntraPoolingIT](../src/test/java/com/boomi/connector/redis/RedisEntraPoolingIT.java))
spin up a throwaway Redis in Docker automatically. For these you just need
**Docker Desktop running** — no config files.

**b) Live-Redis tests** (e.g. [CacheConnectorBasicAuthGetTest](../src/test/java/com/boomi/connector/redis/CacheConnectorBasicAuthGetTest.java))
connect to a Redis *you* point them at, using a properties file. To set one up:

1. Copy the template and drop the `.example` suffix:

   ```powershell
   # Windows PowerShell
   Copy-Item src\test\resources\basicAuth.properties.example src\test\resources\basicAuth.properties
   ```
   ```bash
   # macOS / Linux
   cp src/test/resources/basicAuth.properties.example src/test/resources/basicAuth.properties
   ```

2. Open the new `basicAuth.properties` and fill in your Redis host and
   credentials (`redis.host`, `redis.user`, `redis.password`, …). For Azure /
   Entra tests, do the same with `msEntraAuth.properties.example`.

> **Never commit the real `.properties` files** — only the `.example` templates
> belong in Git. The real ones are already git-ignored because they hold
> credentials.

If you don't have Docker running or haven't created the properties files, those
integration tests will fail or be skipped — that's expected. The build and unit
tests are unaffected.

---

## 7. Upload the connector into Boomi

Once `build` has produced the files in `build/connector-upload/`:

1. In the **Boomi Enterprise Platform**, go to **Settings → Developer**.
2. Create a new connector entry (or edit an existing one).
3. Upload both `BoomiRedisConnector-<version>.car` and `connector-descriptor.xml`.

The connector now appears in the Boomi component palette and can be used in
processes and APIs. (Updating its icon is a separate, optional step — see
[Updating the Connector Icon](updating-connector-icon.md).)

---

## 8. Configure your first connection

Inside Boomi, a connector is used through a **Connection** (where the cache lives
+ how to authenticate) and an **Operation** (what to do — GET, UPSERT, DELETE).

A minimal, no-auth local setup to prove it works:

- **Hosts:** `localhost:6379`
- **Clustering Policy:** `Single Endpoint`
- **Use SSL:** off
- **Authentication Type:** `None`
- **Enable Connection Pooling:** off

Then add a **GET** operation, set the object ID to a key you know exists, and run
it. For the full field-by-field reference — including Basic auth, Azure Cache for
Redis with Microsoft Entra, pooling, and TTL — see the
[main README](../README.md#connection-configuration).

> **Atom Cloud users:** leave connection pooling **off** when running on the
> Boomi Public Runtime Cloud.

---

## 9. Troubleshooting

| Symptom | Likely cause / fix |
|---------|--------------------|
| `'gradlew' is not recognized` / `command not found` | You're not in the project root, or you dropped the `./` (mac/Linux) or `.\` (Windows) prefix. Run from the folder containing `gradlew`. |
| Build fails compiling, mentions a Java version | You have a JRE, not a JDK, or a very old JDK. Install JDK 8+ ([Temurin](https://adoptium.net/)) and re-run. |
| First build hangs or is very slow | Normal on the first run — it's downloading Gradle and libraries. Let it finish; it's cached afterward. |
| `integrationTest` fails with a Docker/connection error | Docker Desktop isn't running (for Testcontainers tests), or your `*.properties` file is missing/incorrect (for live-Redis tests). |
| A test fails and you want details | Open `build/reports/tests/test/index.html`, or add `--info` to the command for more console output. |
| Stale or weird build state | Run `./gradlew clean` then build again. |

---

## 10. Where to go next

- [Main README](../README.md) — full reference for every connection field and
  operation, plus the Azure / Microsoft Entra setup walkthrough.
- Boomi Connector SDK 2.22.1 Javadoc:
  https://boomisdkjavadoc.s3.amazonaws.com/javadoc/2.22.1/index.html
