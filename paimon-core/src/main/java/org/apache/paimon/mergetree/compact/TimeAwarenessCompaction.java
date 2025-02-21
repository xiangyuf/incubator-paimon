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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.mergetree.LevelSortedRun;
import org.apache.paimon.partition.PartitionTimeExtractor;
import org.apache.paimon.partition.PartitionValuesTimeExpireStrategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/** A {@link CompactStrategy} to force compacting level 0 files. */
public class TimeAwarenessCompaction implements CompactStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(TimeAwarenessCompaction.class);

    private final BinaryRow partition;
    private final Duration historicalPartitionThreshold;
    private final CompactStrategy currentCompactStrategy;
    private final CompactStrategy historicalCompactStrategy;
    private final PartitionValuesTimeExpireStrategy partitionValuesTimeExpireStrategy;

    public boolean isHistorical;

    public TimeAwarenessCompaction(
            BinaryRow partition,
            Duration historicalPartitionThreshold,
            CompactStrategy currentCompactStrategy,
            CompactStrategy historicalCompactStrategy,
            PartitionValuesTimeExpireStrategy partitionValuesTimeExpireStrategy) {
        this.partition = partition;
        this.historicalPartitionThreshold = historicalPartitionThreshold;
        this.currentCompactStrategy = currentCompactStrategy;
        this.historicalCompactStrategy = historicalCompactStrategy;
        this.partitionValuesTimeExpireStrategy = partitionValuesTimeExpireStrategy;
        this.isHistorical = isHistorical();
    }

    @Override
    public Optional<CompactUnit> pick(int numLevels, List<LevelSortedRun> runs) {
        if (isHistorical || isHistorical()) {
            return historicalCompactStrategy.pick(numLevels, runs);
        } else {
            return currentCompactStrategy.pick(numLevels, runs);
        }
    }

    private boolean isHistorical() {
        PartitionTimeExtractor partitionTimeExtractor =
                partitionValuesTimeExpireStrategy.getTimeExtractor();
        LocalDateTime partitionDate =
                partitionTimeExtractor.extract(
                        partitionValuesTimeExpireStrategy.getPartitionKeys(),
                        Arrays.asList(
                                partitionValuesTimeExpireStrategy.convertPartition(partition)));
        LocalDateTime historicalPartitionDate =
                LocalDateTime.now().minus(historicalPartitionThreshold);

        boolean result =
                partitionValuesTimeExpireStrategy.isExpired(historicalPartitionDate, partition);
        DateTimeFormatter formatter = DateTimeFormatter.BASIC_ISO_DATE;

        LOG.info(
                "Current partition Date: {}, historical Partition Date: {}, historical result: {}.",
                partitionDate.format(formatter),
                historicalPartitionDate.format(formatter),
                result);

        if (result) {
            isHistorical = true;
        }

        return isHistorical;
    }
}
