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

package com.starrocks.planner;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.load.BrokerFileGroup;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.BrokerDesc;
import com.starrocks.sql.ast.DataDescription;
import com.starrocks.sql.ast.LoadStmt;
import com.starrocks.thrift.TBrokerFileStatus;
import com.starrocks.thrift.TBrokerRangeDesc;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * A lambda argument in a broker load column mapping gets a slot in the source tuple, but it is not a
 * column of the file and is not in src_slot_ids. BE builds its source slots from src_slot_ids and takes
 * the first num_of_columns_from_file of them as file columns, so counting the lambda argument there makes
 * the scanner index past its source slots.
 */
public class FileScanNodeLambdaTest {
    private static ConnectContext connectContext;

    @BeforeAll
    public static void beforeAll() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
        new StarRocksAssert(connectContext).withDatabase("file_scan_lambda").useDatabase("file_scan_lambda")
                .withTable("CREATE TABLE t0 (k1 INT, tags_lower STRING, dt STRING) DUPLICATE KEY(k1) " +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 1 PROPERTIES('replication_num' = '1')");
    }

    @Test
    public void testLambdaArgumentIsNotAFileColumn() throws Exception {
        // k1 and tags come from the file, x does not
        Assertions.assertEquals(2, numOfColumnsFromFile("hdfs://127.0.0.1:9001/file1",
                "(k1, tags) SET (tags_lower = array_join(array_map(x -> lower(x), CAST(tags AS ARRAY<VARCHAR>)), ' '))"));
        // dt comes from the path
        Assertions.assertEquals(2, numOfColumnsFromFile("hdfs://127.0.0.1:9001/dt=2026-01-01/file1",
                "(k1, tags) COLUMNS FROM PATH AS (dt) " +
                        "SET (tags_lower = array_join(array_map(x -> lower(x), CAST(tags AS ARRAY<VARCHAR>)), ' '))"));
    }

    private static int numOfColumnsFromFile(String path, String columns) throws Exception {
        String sql = "LOAD LABEL file_scan_lambda.label0 (DATA INFILE('" + path + "') INTO TABLE t0 " +
                "FORMAT AS 'parquet' " + columns + ") WITH BROKER";
        LoadStmt loadStmt = (LoadStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        DataDescription dataDescription = loadStmt.getDataDescriptions().get(0);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("file_scan_lambda");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "t0");
        BrokerFileGroup fileGroup = new BrokerFileGroup(dataDescription);
        fileGroup.parse(db, dataDescription);

        List<TBrokerFileStatus> fileStatuses = Lists.newArrayList(new TBrokerFileStatus(path, false, 1024, true));
        List<List<TBrokerFileStatus>> fileStatusesList = Lists.newArrayList();
        fileStatusesList.add(fileStatuses);
        DescriptorTable descTable = new DescriptorTable();
        TupleDescriptor tupleDesc = descTable.createTupleDescriptor("DestTableTuple");
        FileScanNode scanNode = new FileScanNode(new PlanNodeId(0), tupleDesc, "FileScanNode",
                fileStatusesList, 1, WarehouseManager.DEFAULT_RESOURCE);
        scanNode.setLoadInfo(1L, 2L, table, new BrokerDesc(null), Lists.newArrayList(fileGroup), false, 1);
        scanNode.setUseVectorizedLoad(true);
        scanNode.init(descTable);
        scanNode.finalizeStats();

        TBrokerRangeDesc range = scanNode.getScanRangeLocations(0).get(0).scan_range.broker_scan_range.ranges.get(0);
        return range.getNum_of_columns_from_file();
    }
}
