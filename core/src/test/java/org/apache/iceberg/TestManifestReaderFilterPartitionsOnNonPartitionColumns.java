/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.TableTestBase.Assertions;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.types.Types.NestedField.required;

@RunWith(Parameterized.class)
public class TestManifestReaderFilterPartitionsOnNonPartitionColumns {

  public static final Schema SCHEMA = new Schema(
          required(3, "id", Types.IntegerType.get()),
          required(4, "data", Types.StringType.get())
  );

  static final FileIO FILE_IO = new TestTables.LocalFileIO();

  // Partition spec used to create tables
  private static final int numBuckets = 10;
  public static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA)
          .bucket("id", numBuckets)
          .build();

  protected final int formatVersion;
  @SuppressWarnings("checkstyle:MemberName")
  protected final Assertions V1Assert;
  @SuppressWarnings("checkstyle:MemberName")
  protected final Assertions V2Assert;
  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
  public TestTables.TestTable table = null;
  protected File tableDir = null;
  protected File metadataDir = null;

  public TestManifestReaderFilterPartitionsOnNonPartitionColumns(int formatVersion) {
    this.formatVersion = formatVersion;
    this.V1Assert = new Assertions(1, formatVersion);
    this.V2Assert = new Assertions(2, formatVersion);
  }

  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[]{1, 2};
  }

  private List<DataFile> buildDataFilesWithMetrics() {
    // We're going to filter "junction".
    List<String> dataValues = Lists.newArrayList("junction", "alligator", "forrest", "clapping",
            "brush", "trap", "element", "limited", "global", "goldfish");
    final Map<Integer, Long> valueCounts = Maps.newHashMap();
    final Map<Integer, Long> nullValueCounts = Maps.newHashMap();
    final Map<Integer, Long> nanValueCounts = Maps.newHashMap();
    nullValueCounts.put(3, 0L);
    nullValueCounts.put(4, 0L);
    valueCounts.put(3, 1L);
    valueCounts.put(4, 1L);
    return IntStream.range(0, numBuckets).mapToObj(i -> {
      final Map<Integer, ByteBuffer> lowerBounds = Maps.newHashMap();
      final Map<Integer, ByteBuffer> upperBounds = Maps.newHashMap();
      lowerBounds.put(4, Conversions.toByteBuffer(Types.StringType.get(), dataValues.get(i)));
      upperBounds.put(4, Conversions.toByteBuffer(Types.StringType.get(), dataValues.get(i)));
      return DataFiles.builder(SPEC)
              .withPath(String.format("/path/to/test_manifest_reader/id_bucket=%d/data-%d.parquet", i, i))
              .withPartitionPath(String.format("id_bucket=%d", i))  // Easy way to set partiion
              .withRecordCount(1)
              .withMetrics(new Metrics(1L, null, valueCounts,
                      nullValueCounts, nanValueCounts, lowerBounds, upperBounds))
              .withFileSizeInBytes(10)
              .build();
    }).collect(Collectors.toList());
  }

  @Before
  public void setupTable() throws Exception {
    this.tableDir = temp.newFolder();
    tableDir.delete(); // created by table create

    this.metadataDir = new File(tableDir, "metadata");
    this.table = create(SCHEMA, SPEC);
  }

  @After
  public void cleanupTables() {
    TestTables.clearTables();
  }

  // TODO(kbendick) - Other classes to check out
  //                     - InclusiveMetricsEvaluator
  //                     - InclusiveManifestEvaluator
  //                     - ProjectionDatumReader
  //                     - AvroIterable
  //                     - FilterIterator
  //                     - org.apache.iceberg.avro.ValueReaders$StructReader
  // This is similar to the spark based test
  // TestFilteredScan.testPartitionedByIdNotStartsWith, but without spark
  // and with more filters.
  @Test
  public void testReaderWithFilterWithoutSelect() throws IOException {
    // Write manifest file directly.
    final List<DataFile> dataFiles = buildDataFilesWithMetrics();
    ManifestFile manifest = writeManifest(1000L, dataFiles.toArray(new DataFile[0]));

    // Set up partition spec ID map so we can use ManifestFiles.read with a filter on it
    Map<Integer, PartitionSpec> specsById = Maps.newHashMap();
    specsById.put(SPEC.specId(), SPEC);

    try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, FILE_IO, specsById)
            .filterRows(Expressions.notStartsWith("data", "junc"))) {

      List<String> files = Streams.stream(reader)
              .map(file -> file.path().toString())
              .collect(Collectors.toList());

      // note that all files are returned because the reader returns data files that may match, and the partition is
      // bucketing by data, which doesn't help filter files
      List<DataFile> expectedDataFiles = Lists.newArrayList(dataFiles).stream()
              .filter(f -> {
                String recordValue = Conversions.fromByteBuffer(Types.StringType.get(), f.upperBounds().get(4));
                return !recordValue.startsWith("junc");
              })
              .collect(Collectors.toList());
      String[] expected = expectedDataFiles.stream().map(f -> f.path().toString()).toArray(String[]::new);
      Assert.assertArrayEquals("Should read the expected files", expected, files.toArray(new String[0]));
    }
  }

  protected TestTables.TestTable create(Schema schema, PartitionSpec spec) {
    return TestTables.create(tableDir, "test", schema, spec, formatVersion);
  }

  public TableMetadata readMetadata() {
    return TestTables.readMetadata("test");
  }

  ManifestFile writeManifest(Long snapshotId, DataFile... files) throws IOException {
    File manifestFile = temp.newFile("input.m0.avro");
    Assert.assertTrue(manifestFile.delete());
    OutputFile outputFile = table.ops().io().newOutputFile(manifestFile.getCanonicalPath());

    ManifestWriter<DataFile> writer = ManifestFiles.write(formatVersion, table.spec(), outputFile, snapshotId);
    try {
      for (DataFile file : files) {
        writer.add(file);
      }
    } finally {
      writer.close();
    }

    return writer.toManifestFile();
  }

  @SuppressWarnings("unchecked")
  <F extends ContentFile<F>> ManifestFile writeManifest(Long snapshotId, String fileName, ManifestEntry<?>... entries)
          throws IOException {
    File manifestFile = temp.newFile(fileName);
    Assert.assertTrue(manifestFile.delete());
    OutputFile outputFile = table.ops().io().newOutputFile(manifestFile.getCanonicalPath());

    ManifestWriter<F> writer;
    if (entries[0].file() instanceof DataFile) {
      writer = (ManifestWriter<F>) ManifestFiles.write(
              formatVersion, table.spec(), outputFile, snapshotId);
    } else {
      writer = (ManifestWriter<F>) ManifestFiles.writeDeleteManifest(
              formatVersion, table.spec(), outputFile, snapshotId);
    }
    try {
      for (ManifestEntry<?> entry : entries) {
        writer.addEntry((ManifestEntry<F>) entry);
      }
    } finally {
      writer.close();
    }

    return writer.toManifestFile();
  }
}
