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

package com.starrocks.analysis;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.IndexAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TOlapTableIndex;
import com.starrocks.type.StringType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class NgramBloomFilterIndexTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
    }

    @Test
    public void testCheckNgramBloomFilterIndexLowerCasesPropertyKeys() {
        Column c = new Column("v", StringType.STRING, true);

        Map<String, String> upperCaseKeys = new HashMap<>();
        upperCaseKeys.put(IndexAnalyzer.GRAM_NUM_KEY.toUpperCase(Locale.ROOT), "3");
        upperCaseKeys.put(IndexAnalyzer.CASE_SENSITIVE_KEY.toUpperCase(Locale.ROOT), "false");
        Assertions.assertDoesNotThrow(
                () -> IndexAnalyzer.checkNgramBloomFilterIndexValid(c, upperCaseKeys, KeysType.PRIMARY_KEYS));
        Assertions.assertEquals("3", upperCaseKeys.get(IndexAnalyzer.GRAM_NUM_KEY));
        Assertions.assertEquals("false", upperCaseKeys.get(IndexAnalyzer.CASE_SENSITIVE_KEY));
        Assertions.assertTrue(upperCaseKeys.keySet().stream()
                .allMatch(key -> key.equals(key.toLowerCase(Locale.ROOT))));

        Map<String, String> collidingKeys = new HashMap<>();
        collidingKeys.put(IndexAnalyzer.GRAM_NUM_KEY.toUpperCase(Locale.ROOT), "3");
        collidingKeys.put(IndexAnalyzer.GRAM_NUM_KEY, "4");
        ExceptionChecker.expectThrowsWithMsg(SemanticException.class,
                "Duplicated index property for NGRAMBF after lower-casing the key: " + IndexAnalyzer.GRAM_NUM_KEY,
                () -> IndexAnalyzer.checkNgramBloomFilterIndexValid(c, collidingKeys, KeysType.PRIMARY_KEYS));
    }

    @Test
    public void testUpperCasePropertyKeysReachBeAsLowerCaseKeys() throws Exception {
        starRocksAssert.withTable("CREATE TABLE `t_upper_case_ngrambf` (\n" +
                "  `k` int NOT NULL COMMENT \"\",\n" +
                "  `v` varchar(50) NOT NULL COMMENT \"\",\n" +
                "  INDEX ngram_v (`v`) USING NGRAMBF(\"GRAM_NUM\" = \"3\", \"CASE_SENSITIVE\" = \"false\")\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k`)\n" +
                "DISTRIBUTED BY HASH(`k`) BUCKETS 1\n" +
                "PROPERTIES (\"replication_num\" = \"1\");");

        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable("test", "t_upper_case_ngrambf");
        TOlapTableIndex olapIndex = table.getIndexes().get(0).toThrift();
        // BE reads these with an exact find(), so any other spelling silently falls back to the defaults.
        Assertions.assertEquals("3", olapIndex.getIndex_properties().get(IndexAnalyzer.GRAM_NUM_KEY));
        Assertions.assertEquals("false", olapIndex.getIndex_properties().get(IndexAnalyzer.CASE_SENSITIVE_KEY));
        Assertions.assertTrue(olapIndex.getIndex_properties().keySet().stream()
                .allMatch(key -> key.equals(key.toLowerCase(Locale.ROOT))));
        starRocksAssert.dropTable("t_upper_case_ngrambf");
    }
}
