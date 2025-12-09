plugins {
    java
    id("org.springframework.boot") version "3.5.6"
    id("io.spring.dependency-management") version "1.1.7"
}

group = "com.viettel"
version = "0.1"
description = "core"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

configurations {
    compileOnly {
        extendsFrom(configurations.annotationProcessor.get())
    }
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.springframework.kafka:spring-kafka")
    implementation("org.springframework.boot:spring-boot-starter-data-jdbc")
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-configuration-processor")
    compileOnly("org.projectlombok:lombok")
    implementation(project(":pm-parser"))

    annotationProcessor("org.projectlombok:lombok")
    implementation("com.clickhouse:clickhouse-jdbc:0.6.5")
    implementation("org.apache.httpcomponents.client5:httpclient5:5.5")
    implementation("org.lz4:lz4-java:1.8.0")
    implementation("org.capnproto:runtime:0.1.3")
    implementation("commons-net:commons-net:3.9.0")
    runtimeOnly("com.mysql:mysql-connector-j")
    implementation("org.apache.commons:commons-pool2:2.11.1")  // quản lí  ftp pool hiệu quả hơn là tự viết
    implementation("org.apache.curator:curator-framework:5.9.0")
    implementation("org.apache.curator:curator-recipes:5.9.0")
    implementation("de.vandermeer:asciitable:0.3.2")
}

//tasks.withType<Test> {
//    useJUnitPlatform()
//}
