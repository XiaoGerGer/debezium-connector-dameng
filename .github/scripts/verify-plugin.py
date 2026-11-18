#!/usr/bin/env python3
# Copyright Debezium Authors.
# Licensed under the Apache Software License, Version 2.0.
"""Check the distributable plugin rather than the Maven test classpath."""

import argparse
import io
from pathlib import Path
import tarfile
import xml.etree.ElementTree as ET
import zipfile

root = Path(__file__).resolve().parents[2]
ns = {"m": "http://maven.apache.org/POM/4.0.0"}
pom = ET.parse(root / "pom.xml").getroot()
artifact = pom.findtext("m:artifactId", namespaces=ns)
version = pom.findtext("m:version", namespaces=ns)
stem = f"{artifact}-{version}"
arguments = argparse.ArgumentParser(description=__doc__)
arguments.add_argument("--distribution-name", default=stem, help="Assembly filename before -plugin.zip / -plugin.tar.gz")
distribution = arguments.parse_args().distribution_name
with zipfile.ZipFile(root / "target" / f"{distribution}-plugin.zip") as archive:
    files = {name for name in archive.namelist() if not name.endswith("/")}
    for notice in ("LICENSE", "README.md"):
        assert archive.read(f"{artifact}/{notice}") == (root / notice).read_bytes(), notice
    jar_bytes = archive.read(f"{artifact}/{stem}.jar")
    assert jar_bytes == (root / "target" / f"{stem}.jar").read_bytes(), "Stale packaged connector"
    jar_names = [Path(name).name for name in files if name.endswith(".jar")]
    assert any(name.startswith("DmJdbcDriver18-") for name in jar_names), "Missing JDBC driver"
    assert any(name.startswith("antlr4-runtime-") for name in jar_names), "Missing ANTLR runtime"
    assert f"debezium-ddl-parser-{version}.jar" in jar_names, "Missing aligned public DDL parser"
    forbidden = ("connect-api-", "connect-runtime-", "kafka-clients-", "slf4j-api-")
    assert not any(name.startswith(forbidden) or name.endswith("-tests.jar") for name in jar_names), jar_names
    parser_owners = {name: [] for name in (
        "io/debezium/antlr/AntlrDdlParser.class",
        "io/debezium/ddl/parser/dameng/generated/DamengSqlParser.class",
    )}
    for name in files:
        if name.endswith(".jar"):
            with zipfile.ZipFile(io.BytesIO(archive.read(name))) as dependency:
                for parser_class, owners in parser_owners.items():
                    if parser_class in dependency.namelist():
                        owners.append(Path(name).name)
    assert parser_owners["io/debezium/antlr/AntlrDdlParser.class"] == [f"debezium-ddl-parser-{version}.jar"], parser_owners
    assert parser_owners["io/debezium/ddl/parser/dameng/generated/DamengSqlParser.class"] == [f"{stem}.jar"], parser_owners
    with zipfile.ZipFile(io.BytesIO(jar_bytes)) as jar:
        assert "io/debezium/ddl/parser/dameng/generated/DamengSqlParser.class" in jar.namelist()
        assert jar.read("io/debezium/connector/dameng/build.version").decode().strip() == f"version={version}"
        assert "META-INF/descriptors/source-connector/io.debezium.connector.dameng.DamengConnector.json" in jar.namelist()
        for service in (
            "org.apache.kafka.connect.source.SourceConnector",
            "io.debezium.metadata.ComponentMetadataProvider",
            "io.debezium.spi.snapshot.Snapshotter",
            "io.debezium.snapshot.spi.SnapshotLock",
            "io.debezium.snapshot.spi.SnapshotQuery",
        ):
            assert jar.read(f"META-INF/services/{service}").strip(), service
    with tarfile.open(root / "target" / f"{distribution}-plugin.tar.gz") as tar:
        assert files == {entry.name for entry in tar if entry.isfile()}, "ZIP/tar file sets differ"
print(f"Verified {distribution}: notices, driver/runtime, generated parser, SPI, descriptor and archive contents")
