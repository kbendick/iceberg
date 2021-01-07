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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.TableTestBase.Assertions;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
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

  static void validateManifestEntries(ManifestFile manifest,
                                      Iterator<Long> ids,
                                      Iterator<DataFile> expectedFiles,
                                      Iterator<ManifestEntry.Status> expectedStatuses) {
    for (ManifestEntry<DataFile> entry : ManifestFiles.read(manifest, FILE_IO).entries()) {
      DataFile file = entry.file();
      DataFile expected = expectedFiles.next();
      final ManifestEntry.Status expectedStatus = expectedStatuses.next();
      Assert.assertEquals("Path should match expected",
              expected.path().toString(), file.path().toString());
      Assert.assertEquals("Snapshot ID should match expected ID",
              ids.next(), entry.snapshotId());
      Assert.assertEquals("Entry status should match expected ID",
              expectedStatus, entry.status());
    }

    Assert.assertFalse("Should find all files in the manifest", expectedFiles.hasNext());
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

  @Test
  // TODO(kbendick) - Other classes to check out
  //                     - InclusiveMetricsEvaluator
  //                     - InclusiveManifestEvaluator
  //                     - ProjectionDatumReader
  //                     - AvroIterable
  //                     - FilterIterator
  //                     - org.apache.iceberg.avro.ValueReaders$StructReader
  // This is an attempt at a reproduction of the spark based test
  // TestFilteredScan.testPartitionedByIdNotStartsWith, but without spark.
  public void testReaderWithFilterWithoutSelect() throws IOException {

    // The element to filter, "junction", occurs at partition_path: id_bucket=0, so we
    // can filter on that when checking the read in files.
    List<String> dataValues = Lists.newArrayList("junction", "alligator", "forrest", "clapping",
            "brush", "trap", "element", "limited", "global", "goldfish");
    final Map<Integer, Long> VALUE_COUNTS = Maps.newHashMap();
    final Map<Integer, Long> NULL_VALUE_COUNTS = Maps.newHashMap();
    final Map<Integer, Long> NAN_VALUE_COUNTS = Maps.newHashMap();
    NULL_VALUE_COUNTS.put(3, 0L);
    NULL_VALUE_COUNTS.put(4, 0L);
    VALUE_COUNTS.put(3, 1L);
    VALUE_COUNTS.put(4, 1L);
    List<DataFile> dataFiles = IntStream.range(0, numBuckets).mapToObj(i -> {
      final Map<Integer, ByteBuffer> LOWER_BOUNDS = Maps.newHashMap();
      final Map<Integer, ByteBuffer> UPPER_BOUNDS = Maps.newHashMap();
      LOWER_BOUNDS.put(4, Conversions.toByteBuffer(Types.StringType.get(), dataValues.get(i)));
      UPPER_BOUNDS.put(4, Conversions.toByteBuffer(Types.StringType.get(), dataValues.get(i)));
      DataFile dataFileForPartition = DataFiles.builder(SPEC)
              .withPath(String.format("/path/to/test_manifest_reader/id_bucket=%d/data-%d.parquet", i, i))
              .withPartitionPath(String.format("id_bucket=%d", i))  // Easy way to set partiion
              .withRecordCount(1)
              .withMetrics(new Metrics(1L, null, VALUE_COUNTS,
                      NULL_VALUE_COUNTS, NAN_VALUE_COUNTS, LOWER_BOUNDS, UPPER_BOUNDS))
              .withFileSizeInBytes(10)  // TODO(kbendick) - Can I just make this up?
              .build();
      return dataFileForPartition;
    }).collect(Collectors.toList());
    ManifestFile manifest = writeManifest(1000L, dataFiles.toArray(new DataFile[0]));

    // Set up partition spec ID map so we can use ManifestFiles.read with a filter on it
    // Set<Integer> partFieldIds = SPEC.fields().stream().map(PartitionField::fieldId).collect(Collectors.toSet());
    Map<Integer, PartitionSpec> specsById = Maps.newHashMap();
    specsById.put(SPEC.specId(), SPEC);
    try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, FILE_IO, specsById)) {
      // .filterPartitions(Expressions.startsWith("data", "junc"))
//                .filterRows(Expressions.notStartsWith("data", "junc"))) {
      reader.filterRows(Expressions.notStartsWith("data", "junc"));
      CloseableIterable<ManifestEntry<DataFile>> entries = reader.entries();
      // This generates a ProjectionDatumReader (if it matters).
      for (ManifestEntry<DataFile> manifestEntry : reader.entries()) {
        DataFile file = manifestEntry.file();
        System.out.printf(
                String.format("DataFile from ManifestEntry - (path, recordCount)(%s, %d)\n", file.path(),
                        file.recordCount()));
        Map<Integer, ByteBuffer> lower = file.lowerBounds();
        Map<Integer, ByteBuffer> upper = file.upperBounds();
        lower.forEach((k, v) ->
                System.out.printf(String.format("\t\tLower Bound for field %d - %s\n",
                        k, Conversions.fromByteBuffer(Types.StringType.get(), v))));
        upper.forEach((k, v) ->
                System.out.printf(String.format("\t\tUpper Bound for field %d - %s\n\n\n",
                        k, Conversions.fromByteBuffer(Types.StringType.get(), v))));
      }

      List<String> files = Streams.stream(reader)
              .map(file -> file.path().toString())
              .collect(Collectors.toList());

      System.err.printf("Reader read %d files\n", files.size());

      // note that all files are returned because the reader returns data files that may match, and the partition is
      // bucketing by data, which doesn't help filter files
      ByteBuffer filteredValueAsBytes = Conversions.toByteBuffer(Types.StringType.get(), "junction");
      List<DataFile> expectedDataFiles = Lists.newArrayList(dataFiles)
              .stream()
              .filter(f -> !f.path().toString().contains("id_bucket=0"))
              .filter(f -> !f.upperBounds().get(4).equals(filteredValueAsBytes)) // No effect.
              .collect(Collectors.toList());
      String[] expected = expectedDataFiles.stream().map(f -> f.path().toString())
              .collect(Collectors.toList()).toArray(new String[0]);
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

  ManifestFile writeManifestWithName(String name, DataFile... files) throws IOException {
    File manifestFile = temp.newFile(name + ".avro");
    Assert.assertTrue(manifestFile.delete());
    OutputFile outputFile = table.ops().io().newOutputFile(manifestFile.getCanonicalPath());

    ManifestWriter<DataFile> writer = ManifestFiles.write(formatVersion, table.spec(), outputFile, null);
    try {
      for (DataFile file : files) {
        writer.add(file);
      }
    } finally {
      writer.close();
    }

    return writer.toManifestFile();
  }

  ManifestEntry<DataFile> manifestEntry(ManifestEntry.Status status, Long snapshotId, DataFile file) {
    GenericManifestEntry<DataFile> entry = new GenericManifestEntry<>(table.spec().partitionType());
    switch (status) {
      case ADDED:
        return entry.wrapAppend(snapshotId, file);
      case EXISTING:
        return entry.wrapExisting(snapshotId, 0L, file);
      case DELETED:
        return entry.wrapDelete(snapshotId, file);
      default:
        throw new IllegalArgumentException("Unexpected entry status: " + status);
    }
  }

  void validateSnapshot(Snapshot old, Snapshot snap, DataFile... newFiles) {
    validateSnapshot(old, snap, null, newFiles);
  }

  void validateSnapshot(Snapshot old, Snapshot snap, long sequenceNumber, DataFile... newFiles) {
    validateSnapshot(old, snap, (Long) sequenceNumber, newFiles);
  }

  void validateSnapshot(Snapshot old, Snapshot snap, Long sequenceNumber, DataFile... newFiles) {
    Assert.assertEquals("Should not change delete manifests",
            old != null ? Sets.newHashSet(old.deleteManifests()) : ImmutableSet.of(),
            Sets.newHashSet(snap.deleteManifests()));
    List<ManifestFile> oldManifests = old != null ? old.dataManifests() : ImmutableList.of();

    // copy the manifests to a modifiable list and remove the existing manifests
    List<ManifestFile> newManifests = Lists.newArrayList(snap.dataManifests());
    for (ManifestFile oldManifest : oldManifests) {
      Assert.assertTrue("New snapshot should contain old manifests",
              newManifests.remove(oldManifest));
    }

    Assert.assertEquals("Should create 1 new manifest and reuse old manifests",
            1, newManifests.size());
    ManifestFile manifest = newManifests.get(0);

    long id = snap.snapshotId();
    Iterator<String> newPaths = paths(newFiles).iterator();

    for (ManifestEntry<DataFile> entry : ManifestFiles.read(manifest, FILE_IO).entries()) {
      DataFile file = entry.file();
      if (sequenceNumber != null) {
        V1Assert.assertEquals("Sequence number should default to 0", 0, entry.sequenceNumber().longValue());
        V2Assert.assertEquals("Sequence number should match expected", sequenceNumber, entry.sequenceNumber());
      }
      Assert.assertEquals("Path should match expected", newPaths.next(), file.path().toString());
      Assert.assertEquals("File's snapshot ID should match", id, (long) entry.snapshotId());
    }

    Assert.assertFalse("Should find all files in the manifest", newPaths.hasNext());
  }

  void validateTableFiles(Table tbl, DataFile... expectedFiles) {
    Set<CharSequence> expectedFilePaths = Sets.newHashSet();
    for (DataFile file : expectedFiles) {
      expectedFilePaths.add(file.path());
    }
    Set<CharSequence> actualFilePaths = Sets.newHashSet();
    for (FileScanTask task : tbl.newScan().planFiles()) {
      actualFilePaths.add(task.file().path());
    }
    Assert.assertEquals("Files should match", expectedFilePaths, actualFilePaths);
  }

  List<String> paths(DataFile... dataFiles) {
    List<String> paths = Lists.newArrayListWithExpectedSize(dataFiles.length);
    for (DataFile file : dataFiles) {
      paths.add(file.path().toString());
    }
    return paths;
  }

  void validateManifest(ManifestFile manifest,
                        Iterator<Long> seqs,
                        Iterator<Long> ids,
                        Iterator<DataFile> expectedFiles,
                        Iterator<ManifestEntry.Status> statuses) {
    for (ManifestEntry<DataFile> entry : ManifestFiles.read(manifest, FILE_IO).entries()) {
      DataFile file = entry.file();
      DataFile expected = expectedFiles.next();
      if (seqs != null) {
        V1Assert.assertEquals("Sequence number should default to 0", 0, entry.sequenceNumber().longValue());
        V2Assert.assertEquals("Sequence number should match expected", seqs.next(), entry.sequenceNumber());
      }
      Assert.assertEquals("Path should match expected",
              expected.path().toString(), file.path().toString());
      Assert.assertEquals("Snapshot ID should match expected ID",
              ids.next(), entry.snapshotId());
      if (statuses != null) {
        Assert.assertEquals("Status should match expected",
                statuses.next(), entry.status());
      }
    }

    Assert.assertFalse("Should find all files in the manifest", expectedFiles.hasNext());
  }

  // TODO(kbendick) - Either remove this or possibly add a test for deleting
  //                  based off of notStartsWith.
  void validateDeleteManifest(ManifestFile manifest,
                              Iterator<Long> seqs,
                              Iterator<Long> ids,
                              Iterator<DeleteFile> expectedFiles,
                              Iterator<ManifestEntry.Status> statuses) {
    for (ManifestEntry<DeleteFile> entry : ManifestFiles.readDeleteManifest(manifest, FILE_IO, null).entries()) {
      DeleteFile file = entry.file();
      DeleteFile expected = expectedFiles.next();
      if (seqs != null) {
        V1Assert.assertEquals("Sequence number should default to 0", 0, entry.sequenceNumber().longValue());
        V2Assert.assertEquals("Sequence number should match expected", seqs.next(), entry.sequenceNumber());
      }
      Assert.assertEquals("Path should match expected",
              expected.path().toString(), file.path().toString());
      Assert.assertEquals("Snapshot ID should match expected ID",
              ids.next(), entry.snapshotId());
      Assert.assertEquals("Status should match expected",
              statuses.next(), entry.status());
    }

    Assert.assertFalse("Should find all files in the manifest", expectedFiles.hasNext());
  }
}
