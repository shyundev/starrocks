// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.sql.plan;

import com.starrocks.qe.ConnectContext;
import com.starrocks.server.RunMode;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * A TopN on the leading range partition column tells the scan to read partitions in the TopN's
 * direction (partition_order_hint), so the TopN runtime filter tightens on the first partitions read.
 * The lake scan node has to carry it as well as the OLAP scan node does.
 */
public class LakeScanOrderHintPlanTest {
    private static ConnectContext connectContext;

    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();

        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("lake_order_hint_test")
                .useDatabase("lake_order_hint_test")
                .withTable("CREATE TABLE lake_order_hint_test.t0 (dt DATE NOT NULL, v1 INT) " +
                        "DUPLICATE KEY(dt) " +
                        "PARTITION BY RANGE(dt) (" +
                        "  PARTITION p1 VALUES [('2026-01-01'), ('2026-01-02'))," +
                        "  PARTITION p2 VALUES [('2026-01-02'), ('2026-01-03'))) " +
                        "DISTRIBUTED BY HASH(v1) BUCKETS 1");
    }

    @Test
    public void testLakeScanNodeCarriesPartitionOrderHint() throws Exception {
        String desc = UtFrameUtils.getPlanThriftString(connectContext,
                "SELECT v1 FROM t0 ORDER BY dt DESC LIMIT 5");
        Assertions.assertTrue(desc.contains("lake_scan_node:"), desc);
        Assertions.assertTrue(desc.contains("partition_order_hint:false"), desc);

        String asc = UtFrameUtils.getPlanThriftString(connectContext,
                "SELECT v1 FROM t0 ORDER BY dt ASC LIMIT 5");
        Assertions.assertTrue(asc.contains("partition_order_hint:true"), asc);
    }
}
