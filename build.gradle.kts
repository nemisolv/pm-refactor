// build.gradle.kts for the parent project
plugins {
    kotlin("jvm") version "1.9.0" // Specify your Kotlin version
    id("io.spring.dependency-management") version "1.1.7"
    java
}

allprojects {
    group = "com.viettel"
    version = "0.1"
    repositories {
        mavenCentral()
    }
}

subprojects {
    apply(plugin = "io.spring.dependency-management")
    apply(plugin = "kotlin")

    java.sourceCompatibility = JavaVersion.VERSION_17 // Specify Java version
    java.targetCompatibility = JavaVersion.VERSION_17

    dependencies {
    }
}
