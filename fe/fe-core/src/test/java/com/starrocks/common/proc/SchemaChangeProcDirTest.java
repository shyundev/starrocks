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

package com.starrocks.common.proc;

import com.google.common.collect.Lists;
import com.starrocks.alter.SchemaChangeHandler;
import com.starrocks.catalog.Database;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.ast.expression.LimitElement;
import mockit.Expectations;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class SchemaChangeProcDirTest {
    @Test
    public void testFetchResultByFilterLimitIsClampedToRows() throws AnalysisException {
        Database db = new Database(10000L, "db1");
        SchemaChangeHandler schemaChangeHandler = new SchemaChangeHandler();
        List<List<Comparable>> infos = Lists.newArrayList();
        for (int jobId = 1; jobId <= 3; jobId++) {
            infos.add(Lists.newArrayList(jobId, "tb" + jobId, "2020-01-01", "2020-01-01", "index", 1, 1, 1, 0,
                    "FINISHED", "", 100, 10000));
        }
        new Expectations(schemaChangeHandler) {
            {
                schemaChangeHandler.getAlterJobInfosByDb(db);
                minTimes = 0;
                result = infos;
            }
        };
        SchemaChangeProcDir dir = new SchemaChangeProcDir(schemaChangeHandler, db);

        Assertions.assertEquals(0, dir.fetchResultByFilter(null, null, new LimitElement(5, 1)).getRows().size());
        Assertions.assertEquals(3,
                dir.fetchResultByFilter(null, null, new LimitElement(0, 4294967296L)).getRows().size());
    }
}
