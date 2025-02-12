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

import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.mergetree.LevelSortedRun;
import org.apache.paimon.mergetree.SortedRun;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import static org.apache.paimon.mergetree.compact.UniversalCompactionTest.file;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ForceUpLevel0Compaction}. */
public class ForceUpLevel0CompactionTest {

    @Test
    public void testForceCompaction0() {
        ForceUpLevel0Compaction compaction =
                new ForceUpLevel0Compaction(new UniversalCompaction(200, 1, 5));

        Optional<CompactUnit> result = compaction.pick(3, Arrays.asList(run(0, 1), run(0, 1)));
        assertThat(result).isPresent();
        assertThat(result.get().outputLevel()).isEqualTo(2);

        result = compaction.pick(3, Arrays.asList(run(0, 1), run(1, 10)));
        assertThat(result).isPresent();
        assertThat(result.get().outputLevel()).isEqualTo(2);

        result = compaction.pick(3, Arrays.asList(run(0, 1), run(0, 5), run(2, 10)));
        assertThat(result).isPresent();
        assertThat(result.get().outputLevel()).isEqualTo(1);

        result = compaction.pick(3, Collections.singletonList(run(2, 10)));
        assertThat(result).isEmpty();

        result = compaction.pick(3, Arrays.asList(run(0, 1), run(0, 5), run(0, 10), run(0, 20)));
        assertThat(result).isPresent();
        assertThat(result.get().outputLevel()).isEqualTo(2);
    }

    @Test
    public void testForceCompaction0WithLateArrivalThreshold() {
        int lateArrivalThresholdDays = 3;
        Duration lateArrivalThreshold = Duration.ofDays(lateArrivalThresholdDays);
        for (int i = 0; i < 30; i++) {
            LocalDateTime testPartitionDate = LocalDateTime.now().minusDays(i);
            ForceUpLevel0Compaction compaction =
                    new ForceUpLevel0Compaction(
                            new UniversalCompaction(200, 1, 5),
                            lateArrivalThreshold,
                            testPartitionDate);

            if (i >= lateArrivalThresholdDays) {
                assertThat(compaction.isLateArrival()).isTrue();

                Optional<CompactUnit> result =
                        compaction.pick(3, Arrays.asList(run(0, 1), run(0, 1)));
                assertThat(result).isEmpty();

                result = compaction.pick(3, Arrays.asList(run(0, 1), run(0, 1), run(1, 10)));
                assertThat(result).isEmpty();

                result =
                        compaction.pick(
                                4,
                                Arrays.asList(
                                        run(0, 1), run(0, 1), run(0, 1), run(1, 10), run(2, 30)));
                assertThat(result).isPresent();
                assertThat(result.get().outputLevel()).isEqualTo(1);
                assertThat(result.get().files().size()).isEqualTo(4);

            } else {
                assertThat(compaction.isLateArrival()).isFalse();

                Optional<CompactUnit> result =
                        compaction.pick(3, Arrays.asList(run(0, 1), run(0, 1)));
                assertThat(result).isPresent();
                assertThat(result.get().outputLevel()).isEqualTo(2);

                result = compaction.pick(3, Arrays.asList(run(0, 1), run(1, 10)));
                assertThat(result).isPresent();
                assertThat(result.get().outputLevel()).isEqualTo(2);

                result = compaction.pick(3, Arrays.asList(run(0, 1), run(0, 5), run(2, 10)));
                assertThat(result).isPresent();
                assertThat(result.get().outputLevel()).isEqualTo(1);
            }
        }
    }

    private LevelSortedRun run(int level, int size) {
        return new LevelSortedRun(level, SortedRun.fromSingle(file(size)));
    }
}
