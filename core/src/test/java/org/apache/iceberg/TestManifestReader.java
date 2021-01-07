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

import com.google.common.collect.Iterables;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.ManifestEntry.Status;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestManifestReader extends TableTestBase {
  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] { 1, 2 };
  }

  public TestManifestReader(int formatVersion) {
    super(formatVersion);
  }

  @Test
  public void testManifestReaderWithEmptyInheritableMetadata() throws IOException {
    ManifestFile manifest = writeManifest(1000L, manifestEntry(Status.EXISTING, 1000L, FILE_A));
    try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, FILE_IO)) {
      ManifestEntry<DataFile> entry = Iterables.getOnlyElement(reader.entries());
      Assert.assertEquals(Status.EXISTING, entry.status());
      Assert.assertEquals(FILE_A.path(), entry.file().path());
      Assert.assertEquals(1000L, (long) entry.snapshotId());
    }
  }

  // TODO(kbendick) - Need to comment this out as it messes with the shared
  //                  hooks. Just using it for easier debugging of certain tasks
  //                  for now.
//  @Test
//  // TODO(kbendick) - Debug and delete me.
//  // TODO - This is like the spark test, but simpler. Should potentially make one that actually
//  // uses an iceberg API scan, etc.
//  public void testReaderWithFilterWithoutSelect() throws IOException {
//
//    final int numBuckets = 10;
//
//    // TODO(kbendick) - This was taken from TestBase.
//    //                  I want someting that is partitioned by id
//    //                  So cannot use the existing SPEC or the existing
//    //                  FILE_A etc as they dont have metrics.
//    final Schema SCHEMA = new Schema(
//            required(3, "id", Types.IntegerType.get()),
//            required(4, "data", Types.StringType.get())
//    );
//
//    // Partition spec used to create tables
//    final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA)
//            .bucket("id", numBuckets)
//            .build();
//
//    // The element to filter, "junction", occurs at partition_path: id_bucket=0, so we
//    // can filter on that when checking the read in files.
//    List<String> dataValues = Lists.newArrayList("junction", "alligator", "forrest", "clapping",
//            "brush", "trap", "element", "limited", "global", "goldfish");
//    final Map<Integer, Long> COLUMN_SIZES = Maps.newHashMap();  // Data not needed
//    final Map<Integer, Long> VALUE_COUNTS = Maps.newHashMap();
//    final Map<Integer, Long> NULL_VALUE_COUNTS = Maps.newHashMap();
//    final Map<Integer, Long> NAN_VALUE_COUNTS = Maps.newHashMap();
//    NULL_VALUE_COUNTS.put(3, 0L);
//    NULL_VALUE_COUNTS.put(4, 0L);
//    VALUE_COUNTS.put(3, 1L);
//    VALUE_COUNTS.put(4, 1L);
//    List<DataFile> dataFiles = IntStream.range(0, numBuckets).mapToObj(i -> {
//      final Map<Integer, ByteBuffer> LOWER_BOUNDS = Maps.newHashMap();
//      final Map<Integer, ByteBuffer> UPPER_BOUNDS = Maps.newHashMap();
//      LOWER_BOUNDS.put(4, Conversions.toByteBuffer(Types.StringType.get(), dataValues.get(i)));
//      UPPER_BOUNDS.put(4, Conversions.toByteBuffer(Types.StringType.get(), dataValues.get(i)));
//      DataFile dataFileForPartition = DataFiles.builder(SPEC)
//              .withPath(String.format("/path/to/test_manifest_reader/id_bucket=%d/data-%d.parquet", i, i))
//              .withPartitionPath(String.format("id_bucket=%d", i))  // Easy way to set partiion
//              .withRecordCount(1)
//              .withMetrics(new Metrics(1L, COLUMN_SIZES, VALUE_COUNTS,
//                      NULL_VALUE_COUNTS, NAN_VALUE_COUNTS, LOWER_BOUNDS, UPPER_BOUNDS))
//              .withFileSizeInBytes(15)  // TODO(kbendick) - Can I just make this up?
//              .build();
//      return dataFileForPartition;
//    }).collect(Collectors.toList());
//    ManifestFile manifest = writeManifest(1000L, dataFiles.toArray(new DataFile[0]));
//    try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, FILE_IO)
//            .filterRows(Expressions.startsWith("data", "junc"))) {
//      List<String> files = Streams.stream(reader)
//              .map(file -> file.path().toString())
//              .collect(Collectors.toList());
//
//      System.err.printf("Reader read %d files\n", files.size());
//
//      // note that all files are returned because the reader returns data files that may match, and the partition is
//      // bucketing by data, which doesn't help filter files
//      ByteBuffer filteredValueAsBytes = Conversions.toByteBuffer(Types.StringType.get(), "junction");
//      List<DataFile> expectedDataFiles = Lists.newArrayList(dataFiles)
//              .stream()
//              .filter(f -> !f.path().toString().contains("id_bucket=0"))
////              .filter(f -> f.upperBounds().get(4).equals(filteredValueAsBytes)) // No effect.
//              .collect(Collectors.toList());
//      String[] expected = expectedDataFiles.stream().map(f -> f.path().toString())
//              .collect(Collectors.toList()).toArray(new String[0]);
//      Assert.assertArrayEquals("Should read the expected files", expected, files.toArray(new String[0]));
//    }
//  }

//  @Test
//  // TODO - This is like the spark test, but simpler. Should potentially make one that actually
//  // uses an iceberg API scan, etc.
//  public void testReaderWithFilterWithScan() throws IOException {
//
//    final int numBuckets = 10;
//
//    // TODO(kbendick) - This was taken from TestBase.
//    //                  I want someting that is partitioned by id
//    //                  So cannot use the existing SPEC or the existing
//    //                  FILE_A etc as they dont have metrics.
//    final Schema SCHEMA = new Schema(
//            required(3, "id", Types.IntegerType.get()),
//            required(4, "data", Types.StringType.get())
//    );
//
//    // Partition spec used to create tables
//    final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA)
//            .bucket("id", numBuckets)
//            .build();
//
//    // Build up the files
//    //
//    // The element to filter, "junction", occurs at partition_path: id_bucket=0, so we
//    // can filter on that when checking the read in files.
//    List<String> dataValues = Lists.newArrayList("junction", "alligator", "forrest", "clapping",
//            "brush", "trap", "element", "limited", "global", "goldfish");
//    final Map<Integer, Long> COLUMN_SIZES = Maps.newHashMap();  // Data not needed
//    final Map<Integer, Long> VALUE_COUNTS = Maps.newHashMap();
//    final Map<Integer, Long> NULL_VALUE_COUNTS = Maps.newHashMap();
//    final Map<Integer, Long> NAN_VALUE_COUNTS = Maps.newHashMap();
//    NULL_VALUE_COUNTS.put(3, 0L);
//    NULL_VALUE_COUNTS.put(4, 0L);
//    VALUE_COUNTS.put(3, 1L);
//    VALUE_COUNTS.put(4, 1L);
//    List<DataFile> dataFiles = IntStream.range(0, numBuckets).mapToObj(i -> {
//      final Map<Integer, ByteBuffer> LOWER_BOUNDS = Maps.newHashMap();
//      final Map<Integer, ByteBuffer> UPPER_BOUNDS = Maps.newHashMap();
//      LOWER_BOUNDS.put(4, Conversions.toByteBuffer(Types.StringType.get(), dataValues.get(i)));
//      UPPER_BOUNDS.put(4, Conversions.toByteBuffer(Types.StringType.get(), dataValues.get(i)));
//      DataFile dataFileForPartition = DataFiles.builder(SPEC)
//              .withPath(String.format("/path/to/testReaderWithFilterWithoutSelect/id_bucket=%d/data-%d.parquet", i, i))
//              .withPartitionPath(String.format("id_bucket=%d", i))  // Easy way to set partiion
//              .withRecordCount(1)
//              .withMetrics(new Metrics(1L, COLUMN_SIZES, VALUE_COUNTS,
//                      NULL_VALUE_COUNTS, NAN_VALUE_COUNTS, LOWER_BOUNDS, UPPER_BOUNDS))
//              .withFileSizeInBytes(15)  // TODO(kbendick) - Can I just make this up?
//              .build();
//      return dataFileForPartition;
//    }).collect(Collectors.toList());
//
////    ManifestFile manifest = writeManifest(1L, dataFiles.toArray(new DataFile[0]));
//
////    Assert.assertNotNull("Manifest's snapshot Id should be non-null", manifest.snapshotId());
//
//    // this.tableDir.delete();
//    // this.tableDir = temp.newFolder();
//    // tableDir.delete(); // created by table create
//
//    this.metadataDir = new File(tableDir, "metadata");
//    this.table = create(SCHEMA, SPEC);
//
//    AppendFiles append = table.newAppend();
//    for (DataFile f : dataFiles) {
//      append.appendFile(f);
//    }
//    append.commit();
//
////    append.appendManifest(manifest);
////    append.commit();
//
////    append.appendManifest(manifest).commit();
//
//    validateTableFiles(this.table, dataFiles.toArray(new DataFile[0]));
//
//    TableScan scan = this.table.newScan().filter(Expressions.startsWith("data", "junc"));
//    CloseableIterable<FileScanTask> closeableFileScanTasks = scan.planFiles();
//
//    List<FileScanTask> fileScanTasks = Lists.newArrayList();
//    Iterables.addAll(fileScanTasks, closeableFileScanTasks);
//
//    Assert.assertEquals("Filtered table scan should result in correct number of file scan tasks",
//            10, fileScanTasks.size());
//
//    CloseableIterable<CombinedScanTask> combinedScanTasks = TableScanUtil.planTasks(closeableFileScanTasks,
//            10L, 10, 94964L);
//
//    List<CombinedScanTask> cst = Lists.newArrayList();
//    Iterables.addAll(cst, combinedScanTasks);
//
//    for (CombinedScanTask ct : cst) {
//      List<FileScanTask> files = ct.files().stream().collect(Collectors.toList());
//      files.stream().map(f -> f.file().path()).forEach(p -> System.out.println("File to be processed " + p));
//    }
//
//    // Why is this two??
//    Assert.assertEquals("Combined file scan tasks should have filtered out one partition",
//            2, cst.size());
//
//    // defer
//    closeableFileScanTasks.close();
//    combinedScanTasks.close();
//
//
////    table.newAppend()
////            .appendFile(FILE_A)
////            .commit();
////
////    table.newOverwrite()
////            .deleteFile(FILE_A)
////            .addFile(FILE_B)
////            .stageOnly()
////            .commit();
//
////    // the overwrite should only be staged
////    validateTableFiles(table, FILE_A);
////
////    Snapshot overwrite = Streams.stream(table.snapshots())
////            .filter(snap -> DataOperations.OVERWRITE.equals(snap.operation()))
////            .findFirst()
////            .get();
//
//    // cherry-pick the overwrite; this works because it is a fast-forward commit
////    table.manageSnapshots().cherrypick(overwrite.snapshotId()).commit();
//
//    // the overwrite should only be staged
////    validateTableFiles(table, FILE_B);
//
////    try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, FILE_IO)
////            .filterRows(Expressions.startsWith("data", "junc"))) {
////      List<String> files = Streams.stream(reader)
////              .map(file -> file.path().toString())
////              .collect(Collectors.toList());
////
////      System.err.printf("Reader read %d files\n", files.size());
////
////      // note that all files are returned because the reader returns data files that may match, and the partition is
////      // bucketing by data, which doesn't help filter files
////      ByteBuffer filteredValueAsBytes = Conversions.toByteBuffer(Types.StringType.get(), "junction");
////      List<DataFile> expectedDataFiles = Lists.newArrayList(dataFiles)
////              .stream()
////              .filter(f -> !f.path().toString().contains("id_bucket=0"))
//////              .filter(f -> f.upperBounds().get(4).equals(filteredValueAsBytes))  // This filter should have no effect
////              .collect(Collectors.toList());
////      String[] expected = expectedDataFiles.stream().map(f -> f.path().toString())
////              .collect(Collectors.toList()).toArray(new String[0]);
////      Assert.assertArrayEquals("Should read the expected files", expected, files.toArray(new String[0]));
////    }
//  }

  @Test
  public void testInvalidUsage() throws IOException {
    ManifestFile manifest = writeManifest(FILE_A, FILE_B);
    AssertHelpers.assertThrows(
        "Should not be possible to read manifest without explicit snapshot ids and inheritable metadata",
        IllegalArgumentException.class, "Cannot read from ManifestFile with null (unassigned) snapshot ID",
        () -> ManifestFiles.read(manifest, FILE_IO));
  }

  @Test
  public void testManifestReaderWithPartitionMetadata() throws IOException {
    ManifestFile manifest = writeManifest(1000L, manifestEntry(Status.EXISTING, 123L, FILE_A));
    try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, FILE_IO)) {
      ManifestEntry<DataFile> entry = Iterables.getOnlyElement(reader.entries());
      Assert.assertEquals(123L, (long) entry.snapshotId());

      List<Types.NestedField> fields = ((PartitionData) entry.file().partition()).getPartitionType().fields();
      Assert.assertEquals(1, fields.size());
      Assert.assertEquals(1000, fields.get(0).fieldId());
      Assert.assertEquals("data_bucket", fields.get(0).name());
      Assert.assertEquals(Types.IntegerType.get(), fields.get(0).type());
    }
  }

  @Test
  public void testManifestReaderWithUpdatedPartitionMetadataForV1Table() throws IOException {
    PartitionSpec spec = PartitionSpec.builderFor(table.schema())
        .bucket("id", 8)
        .bucket("data", 16)
        .build();
    table.ops().commit(table.ops().current(), table.ops().current().updatePartitionSpec(spec));

    ManifestFile manifest = writeManifest(1000L, manifestEntry(Status.EXISTING, 123L, FILE_A));
    try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, FILE_IO)) {
      ManifestEntry<DataFile> entry = Iterables.getOnlyElement(reader.entries());
      Assert.assertEquals(123L, (long) entry.snapshotId());

      List<Types.NestedField> fields = ((PartitionData) entry.file().partition()).getPartitionType().fields();
      Assert.assertEquals(2, fields.size());
      Assert.assertEquals(1000, fields.get(0).fieldId());
      Assert.assertEquals("id_bucket", fields.get(0).name());
      Assert.assertEquals(Types.IntegerType.get(), fields.get(0).type());

      Assert.assertEquals(1001, fields.get(1).fieldId());
      Assert.assertEquals("data_bucket", fields.get(1).name());
      Assert.assertEquals(Types.IntegerType.get(), fields.get(1).type());
    }
  }

  @Test
  public void testDataFilePositions() throws IOException {
    ManifestFile manifest = writeManifest(1000L, FILE_A, FILE_B, FILE_C);
    try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, FILE_IO)) {
      long expectedPos = 0L;
      for (DataFile file : reader) {
        Assert.assertEquals("Position should match", (Long) expectedPos, file.pos());
        expectedPos += 1;
      }
    }
  }
}
