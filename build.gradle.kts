// seatwave-analytics (Module 31). Versions are in gradle/libs.versions.toml, checked on Maven Central
// on 13 November 2026.
plugins {
    java
    jacoco
    alias(libs.plugins.spotless)
}

group = "io.plainround"
version = "1.0.0"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
}

// Two source sets. "main" is the teaching model: the generator, the windows, the batch model, the
// reconciliation and the four programs, on the JDK alone. "kafka" is the real path: the Kafka
// Streams job, the publisher, the archiver, the DuckDB load and the reconciliation of the two, which
// need what the JDK lacks.
val kafka: SourceSet by sourceSets.creating {
    java.srcDir("kafka/src/main/java")
    resources.srcDir("kafka/src/main/resources")
    // The warehouse SQL is read from the class path, so the load runs from any folder.
    resources.srcDir("warehouse")
    compileClasspath += sourceSets.main.get().output
    runtimeClasspath += sourceSets.main.get().output
}

sourceSets.test {
    compileClasspath += kafka.output
    runtimeClasspath += kafka.output
}

configurations {
    named("testImplementation") { extendsFrom(configurations["kafkaImplementation"]) }
    named("testRuntimeOnly") { extendsFrom(configurations["kafkaRuntimeOnly"]) }
}

dependencies {
    // Windows, state stores, repartitioning and transactional commits over Kafka.
    "kafkaImplementation"(libs.kafka.streams)
    constraints {
        "kafkaImplementation"(libs.lz4.java) { because("lz4-java 1.11.2 has advisories; fixed in 1.11.4") }
        "kafkaImplementation"(libs.snappy.java) { because("snappy-java 1.1.10.7 has advisories; fixed in 1.1.10.9") }
    }
    // An embedded columnar SQL warehouse; it also writes the lake's Parquet files, so no Parquet
    // library (and no Hadoop tree) is needed.
    "kafkaImplementation"(libs.duckdb.jdbc)
    // The row store that the column store is measured against, embedded like DuckDB.
    "kafkaImplementation"(libs.h2)
    // Module 28's events are JSON objects on the topic.
    "kafkaImplementation"(platform(libs.jackson.bom))
    "kafkaImplementation"(libs.jackson.databind)
    "kafkaImplementation"(libs.slf4j.api)
    "kafkaRuntimeOnly"(libs.logback.classic)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj.core)
    // TopologyTestDriver runs the real topology without a broker.
    testImplementation(libs.kafka.streams.test.utils)
    testRuntimeOnly(libs.junit.platform.launcher)
}

// Transitive versions are written to gradle.lockfile, so two builds can never resolve different jars.
dependencyLocking {
    lockAllConfigurations()
}

tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
    options.release = 25
    // Every warning is an error: a warning that is allowed to stay is a defect nobody owns.
    options.compilerArgs.addAll(listOf("-Xlint:all", "-Werror"))
}

spotless {
    java {
        target("src/*/java/**/*.java", "kafka/src/*/java/**/*.java")
        googleJavaFormat(libs.versions.googleJavaFormat.get())
        removeUnusedImports()
        endWithNewline()
    }
}

jacoco {
    toolVersion = libs.versions.jacoco.get()
}

tasks.test {
    useJUnitPlatform()
    // The invariant test generates a tenth of the on-sale (137,763 deliveries) several times.
    maxHeapSize = "1g"
    // DuckDB's driver loads its native library; Java 25 warns unless native access is granted.
    jvmArgs("--enable-native-access=ALL-UNNAMED")
    testLogging {
        events("failed")
    }
    finalizedBy(tasks.jacocoTestReport)
}

// The programs print and exit, and the stack adapters (Kafka clients, the archiver's consumer) run
// only against the stack: both are measured by the stack run, not by ./gradlew check.
val stackOnly = listOf("**/*Cli*", "**/kafka/StackClients*")

// The report and the floor cover the same classes: both source sets, without the stack-only ones.
val covered = (sourceSets.main.get().output.asFileTree + kafka.output.asFileTree).matching { exclude(stackOnly) }

tasks.jacocoTestReport {
    sourceDirectories.setFrom(sourceSets.main.get().allJava.srcDirs + kafka.allJava.srcDirs)
    classDirectories.setFrom(covered)
    reports {
        xml.required = true
    }
}

tasks.jacocoTestCoverageVerification {
    dependsOn(tasks.test)
    sourceDirectories.setFrom(sourceSets.main.get().allJava.srcDirs + kafka.allJava.srcDirs)
    classDirectories.setFrom(covered)
    violationRules {
        rule {
            limit {
                counter = "LINE"
                minimum = "0.80".toBigDecimal()
            }
        }
    }
}

tasks.check {
    dependsOn(tasks.jacocoTestCoverageVerification)
}

// One task per teaching program: ./gradlew windowsRun checkpointRun reconcileRun layoutRun
listOf("Windows", "Checkpoint", "Reconcile", "Layout").forEach { program ->
    tasks.register<JavaExec>("${program.lowercase()}Run") {
        group = "seatwave"
        classpath = sourceSets.main.get().runtimeClasspath
        mainClass = "io.plainround.seatwave.analytics.${program}Cli"
        maxHeapSize = "3g" // 1,377,633 deliveries are held in memory at once
    }
}

// One task per stack program (arguments through --args). scripts/stack.sh starts the same
// programs with plain java, so that a run can stop the stream job by its process id.
mapOf(
    "streamRun" to "kafka.StreamCli",
    "publishOnSale" to "kafka.PublishCli",
    "archiveRun" to "kafka.ArchiverCli",
    "lagProbe" to "kafka.LagProbeCli",
    "nightlyLoad" to "warehouse.NightlyLoadCli",
    "reconcile" to "warehouse.SalesReconciliationCli",
    "layoutDbRun" to "warehouse.LayoutDbCli",
).forEach { (taskName, program) ->
    tasks.register<JavaExec>(taskName) {
        group = "seatwave"
        classpath = kafka.runtimeClasspath
        mainClass = "io.plainround.seatwave.analytics.$program"
        workingDir = projectDir
        jvmArgs("--enable-native-access=ALL-UNNAMED")
        // The publisher and the column-store comparison hold a whole on-sale in memory.
        maxHeapSize = if (taskName == "layoutDbRun" || taskName == "publishOnSale") "3g" else "1g"
    }
}

// Writes the class path of the stack programs as one line, for scripts/stack.sh.
tasks.register("stackClasspath") {
    group = "seatwave"
    description = "Compiles the code and writes build/stack/runtime.classpath for scripts/stack.sh."
    val runtime = kafka.runtimeClasspath
    val target = layout.buildDirectory.file("stack/runtime.classpath")
    inputs.files(runtime)
    outputs.file(target)
    doLast {
        target.get().asFile.writeText(runtime.files.joinToString(File.pathSeparator, postfix = "\n") { it.absolutePath })
    }
}
