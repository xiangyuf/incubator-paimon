/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.mergetree.compact;

import static org.apache.paimon.io.DataFileTestUtils.newFile;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileTestUtils;
import org.apache.paimon.mergetree.Levels;
import org.apache.paimon.mergetree.SortedRun;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/** Test for {@link MergeTreeCompactManager}. */
public class MergeTreeCompactManagerTest {

    private final Comparator<InternalRow> comparator = Comparator.comparingInt(o -> o.getInt(0));

    private static ExecutorService service;

    @BeforeAll
    public static void before() {
        service = Executors.newSingleThreadExecutor();
    }

    @AfterAll
    public static void after() {
        service.shutdownNow();
        service = null;
    }

    @Test
    public void testOutputToZeroLevel() throws ExecutionException, InterruptedException {
        innerTest(
                Arrays.asList(
                        new LevelMinMax(0, 1, 3),
                        new LevelMinMax(0, 1, 5),
                        new LevelMinMax(0, 1, 8)),
                Arrays.asList(new LevelMinMax(0, 1, 8), new LevelMinMax(0, 1, 3)),
                (numLevels, runs) -> Optional.of(CompactUnit.fromLevelRuns(0, runs.subList(0, 2))),
                false);
    }

    @Test
    public void testCompactToPenultimateLayer() throws ExecutionException, InterruptedException {
        innerTest(
                Arrays.asList(
                        new LevelMinMax(0, 1, 3),
                        new LevelMinMax(0, 1, 5),
                        new LevelMinMax(2, 1, 7)),
                Arrays.asList(new LevelMinMax(1, 1, 5), new LevelMinMax(2, 1, 7)),
                (numLevels, runs) -> Optional.of(CompactUnit.fromLevelRuns(1, runs.subList(0, 2))),
                false);
    }

    @Test
    public void testNoCompaction() throws ExecutionException, InterruptedException {
        innerTest(
                Collections.singletonList(new LevelMinMax(3, 1, 3)),
                Collections.singletonList(new LevelMinMax(3, 1, 3)));
    }

    @Test
    public void testNormal() throws ExecutionException, InterruptedException {
        innerTest(
                Arrays.asList(
                        new LevelMinMax(0, 1, 3),
                        new LevelMinMax(1, 1, 5),
                        new LevelMinMax(1, 6, 7)),
                Arrays.asList(new LevelMinMax(2, 1, 5), new LevelMinMax(2, 6, 7)));
    }

    @Test
    public void testUpgrade() throws ExecutionException, InterruptedException {
        innerTest(
                Arrays.asList(
                        new LevelMinMax(0, 1, 3),
                        new LevelMinMax(0, 1, 5),
                        new LevelMinMax(0, 6, 8)),
                Arrays.asList(new LevelMinMax(2, 1, 5), new LevelMinMax(2, 6, 8)));
    }

    @Test
    public void testSmallFiles() throws ExecutionException, InterruptedException {
        innerTest(
                Arrays.asList(new LevelMinMax(0, 1, 1), new LevelMinMax(0, 2, 2)),
                Collections.singletonList(new LevelMinMax(2, 1, 2)));
    }

    @Test
    public void testSmallFilesNoCompact() throws ExecutionException, InterruptedException {
        innerTest(
                Arrays.asList(
                        new LevelMinMax(0, 1, 5),
                        new LevelMinMax(0, 6, 6),
                        new LevelMinMax(1, 7, 8),
                        new LevelMinMax(1, 9, 10)),
                Arrays.asList(
                        new LevelMinMax(2, 1, 5),
                        new LevelMinMax(2, 6, 6),
                        new LevelMinMax(2, 7, 8),
                        new LevelMinMax(2, 9, 10)));
    }

    @Test
    public void testSmallFilesCrossLevel() throws ExecutionException, InterruptedException {
        innerTest(
                Arrays.asList(
                        new LevelMinMax(0, 1, 5),
                        new LevelMinMax(0, 6, 6),
                        new LevelMinMax(1, 7, 7),
                        new LevelMinMax(1, 9, 10)),
                Arrays.asList(
                        new LevelMinMax(2, 1, 5),
                        new LevelMinMax(2, 6, 7),
                        new LevelMinMax(2, 9, 10)));
    }

    @Test
    public void testComplex() throws ExecutionException, InterruptedException {
        innerTest(
                Arrays.asList(
                        new LevelMinMax(0, 1, 5),
                        new LevelMinMax(0, 6, 6),
                        new LevelMinMax(1, 1, 4),
                        new LevelMinMax(1, 6, 8),
                        new LevelMinMax(1, 10, 11),
                        new LevelMinMax(2, 1, 3),
                        new LevelMinMax(2, 4, 6)),
                Arrays.asList(new LevelMinMax(2, 1, 8), new LevelMinMax(2, 10, 11)));
    }

    @Test
    public void testSmallInComplex() throws ExecutionException, InterruptedException {
        innerTest(
                Arrays.asList(
                        new LevelMinMax(0, 1, 5),
                        new LevelMinMax(0, 6, 6),
                        new LevelMinMax(1, 1, 4),
                        new LevelMinMax(1, 6, 8),
                        new LevelMinMax(1, 10, 10),
                        new LevelMinMax(2, 1, 3),
                        new LevelMinMax(2, 4, 6)),
                Collections.singletonList(new LevelMinMax(2, 1, 10)));
    }

    @Test
    public void testHistoricalCompaction() throws ExecutionException, InterruptedException {
        List<LevelMinMax> inputs =
                Arrays.asList(
                        new LevelMinMax(0, 1, 5),
                        new LevelMinMax(0, 3, 6),
                        new LevelMinMax(0, 6, 8),
                        new LevelMinMax(1, 1, 4),
                        new LevelMinMax(1, 6, 8),
                        new LevelMinMax(1, 10, 10),
                        new LevelMinMax(2, 1, 3),
                        new LevelMinMax(2, 4, 6));
        List<DataFileMeta> files1 = new ArrayList<>();
        List<DataFileMeta> files2 = new ArrayList<>();
        for (int i = 0; i < inputs.size(); i++) {
            LevelMinMax minMax = inputs.get(i);
            files1.add(minMax.toFile(i));
            files2.add(minMax.toFile(i));
        }

        Levels levels1 = new Levels(comparator, files1, 3);
        Levels levels2 = new Levels(comparator, files2, 3);

        CompactStrategy strategy = new UniversalCompaction(0, 0, 5);

        MergeTreeCompactManager oldManager =
                new MergeTreeCompactManager(
                        service,
                        levels1,
                        strategy,
                        comparator,
                        2,
                        Integer.MAX_VALUE,
                        new TestRewriter(true),
                        null,
                        null,
                        false,
                        Duration.ofDays(2),
                        6,
                        LocalDateTime.now().minusDays(2));

        MergeTreeCompactManager newManager =
                new MergeTreeCompactManager(
                        service,
                        levels2,
                        strategy,
                        comparator,
                        2,
                        Integer.MAX_VALUE,
                        new TestRewriter(true),
                        null,
                        null,
                        false,
                        Duration.ofDays(2),
                        6,
                        LocalDateTime.now().minusDays(1));

        assertThat(oldManager.shouldTriggerCompactForHistorical()).isFalse();
        oldManager.triggerCompaction(false);
        assertThat(oldManager.getCompactionResult(true)).isEmpty();

        oldManager.triggerCompaction(false);
        oldManager.triggerCompaction(false);
        oldManager.triggerCompaction(false);
        oldManager.triggerCompaction(false);

        assertThat(oldManager.shouldTriggerCompactForHistorical()).isTrue();
        oldManager.triggerCompaction(false);
        assertThat(oldManager.getCompactionResult(true)).isPresent();

        assertThat(newManager.shouldTriggerCompactForHistorical()).isFalse();
        newManager.triggerCompaction(false);
        assertThat(newManager.getCompactionResult(true)).isPresent();
        newManager.triggerCompaction(false);
        newManager.triggerCompaction(false);
        newManager.triggerCompaction(false);
        newManager.triggerCompaction(false);
        newManager.triggerCompaction(false);
        newManager.triggerCompaction(false);
        assertThat(newManager.shouldTriggerCompactForHistorical()).isFalse();

        oldManager.addNewFile((new LevelMinMax(0, 11, 12)).toFile(inputs.size()));
        oldManager.addNewFile((new LevelMinMax(0, 13, 14)).toFile(inputs.size() + 1));
        oldManager.addNewFile((new LevelMinMax(0, 15, 16)).toFile(inputs.size() + 2));
        oldManager.addNewFile((new LevelMinMax(0, 17, 18)).toFile(inputs.size() + 3));
        assertThat(oldManager.shouldTriggerCompactForHistorical()).isFalse();
        oldManager.addNewFile((new LevelMinMax(0, 15, 16)).toFile(inputs.size() + 4));
        oldManager.addNewFile((new LevelMinMax(0, 17, 18)).toFile(inputs.size() + 6));
        assertThat(oldManager.shouldTriggerCompactForHistorical()).isTrue();
        oldManager.triggerCompaction(false);
        assertThat(oldManager.getCompactionResult(true)).isPresent();

        oldManager.triggerCompaction(false);
        assertThat(oldManager.shouldTriggerCompactForHistorical()).isFalse();
        oldManager.triggerCompaction(false);
        oldManager.triggerCompaction(false);
        oldManager.triggerCompaction(false);
        oldManager.triggerCompaction(false);
        assertThat(oldManager.shouldTriggerCompactForHistorical()).isTrue();
    }

    private void innerTest(List<LevelMinMax> inputs, List<LevelMinMax> expected)
            throws ExecutionException, InterruptedException {
        innerTest(inputs, expected, testStrategy(), true);
    }

    private void innerTest(
            List<LevelMinMax> inputs,
            List<LevelMinMax> expected,
            CompactStrategy strategy,
            boolean expectedDropDelete)
            throws ExecutionException, InterruptedException {
        List<DataFileMeta> files = new ArrayList<>();
        for (int i = 0; i < inputs.size(); i++) {
            LevelMinMax minMax = inputs.get(i);
            files.add(minMax.toFile(i));
        }
        Levels levels = new Levels(comparator, files, 3);
        MergeTreeCompactManager manager =
                new MergeTreeCompactManager(
                        service,
                        levels,
                        strategy,
                        comparator,
                        2,
                        Integer.MAX_VALUE,
                        new TestRewriter(expectedDropDelete),
                        null,
                        null,
                        false);
        manager.triggerCompaction(false);
        manager.getCompactionResult(true);
        List<LevelMinMax> outputs =
                levels.allFiles().stream().map(LevelMinMax::new).collect(Collectors.toList());
        assertThat(outputs).isEqualTo(expected);
    }

    public static BinaryRow row(int i) {
        return DataFileTestUtils.row(i);
    }

    private CompactStrategy testStrategy() {
        return (numLevels, runs) -> Optional.of(CompactUnit.fromLevelRuns(numLevels - 1, runs));
    }

    private static class TestRewriter extends AbstractCompactRewriter {

        private final boolean expectedDropDelete;

        private TestRewriter(boolean expectedDropDelete) {
            this.expectedDropDelete = expectedDropDelete;
        }

        @Override
        public CompactResult rewrite(
                int outputLevel, boolean dropDelete, List<List<SortedRun>> sections)
                throws Exception {
            assertThat(dropDelete).isEqualTo(expectedDropDelete);
            int minKey = Integer.MAX_VALUE;
            int maxKey = Integer.MIN_VALUE;
            long maxSequence = 0;
            for (List<SortedRun> section : sections) {
                for (SortedRun run : section) {
                    for (DataFileMeta file : run.files()) {
                        int min = file.minKey().getInt(0);
                        int max = file.maxKey().getInt(0);
                        if (min < minKey) {
                            minKey = min;
                        }
                        if (max > maxKey) {
                            maxKey = max;
                        }
                        if (file.maxSequenceNumber() > maxSequence) {
                            maxSequence = file.maxSequenceNumber();
                        }
                    }
                }
            }
            return new CompactResult(
                    extractFilesFromSections(sections),
                    Collections.singletonList(newFile(outputLevel, minKey, maxKey, maxSequence)));
        }
    }

    private static class LevelMinMax {

        private final int level;
        private final int min;
        private final int max;

        private LevelMinMax(DataFileMeta file) {
            this.level = file.level();
            this.min = file.minKey().getInt(0);
            this.max = file.maxKey().getInt(0);
        }

        private LevelMinMax(int level, int min, int max) {
            this.level = level;
            this.min = min;
            this.max = max;
        }

        private DataFileMeta toFile(long maxSequence) {
            return newFile(level, min, max, maxSequence);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            LevelMinMax that = (LevelMinMax) o;
            return level == that.level && min == that.min && max == that.max;
        }

        @Override
        public int hashCode() {
            return Objects.hash(level, min, max);
        }

        @Override
        public String toString() {
            return "LevelMinMax{" + "level=" + level + ", min=" + min + ", max=" + max + '}';
        }
    }
}
