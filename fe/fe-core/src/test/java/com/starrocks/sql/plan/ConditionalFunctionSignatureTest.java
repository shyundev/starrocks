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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ConditionalFunctionSignatureTest extends PlanTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        starRocksAssert.withTable("create table tsig (id int, b1 varbinary, b2 varbinary, " +
                "d1 decimal(50, 2), d2 decimal(50, 2)) duplicate key(id) distributed by hash(id) buckets 1 " +
                "properties('replication_num' = '1')");
    }

    @Test
    public void testVarbinaryArgumentsKeepType() throws Exception {
        assertContains(getFragmentPlan("select if(id = 1, b1, b2) from tsig"), "if(1: id = 1, 2: b1, 3: b2)");
        assertContains(getFragmentPlan("select if(id = 1, b1, null) from tsig"), "if(1: id = 1, 2: b1, NULL)");
        assertContains(getFragmentPlan("select ifnull(b1, b2) from tsig"), "ifnull(2: b1, 3: b2)");
        assertContains(getFragmentPlan("select nullif(b1, b2) from tsig"), "nullif(2: b1, 3: b2)");
        assertContains(getFragmentPlan("select coalesce(b1, b2) from tsig"), "coalesce(2: b1, 3: b2)");
        assertContains(getFragmentPlan("select greatest(b1, b2) from tsig"), "greatest(2: b1, 3: b2)");
        assertContains(getFragmentPlan("select least(b1, b2) from tsig"), "least(2: b1, 3: b2)");
    }

    @Test
    public void testDecimal256ArgumentsKeepType() throws Exception {
        assertContains(getFragmentPlan("select if(id = 1, d1, d2) from tsig"), "if(1: id = 1, 4: d1, 5: d2)");
        assertContains(getFragmentPlan("select ifnull(d1, d2) from tsig"), "ifnull(4: d1, 5: d2)");
        assertContains(getFragmentPlan("select nullif(d1, d2) from tsig"), "nullif(4: d1, 5: d2)");
        assertContains(getFragmentPlan("select coalesce(d1, d2) from tsig"), "coalesce(4: d1, 5: d2)");
        assertContains(getFragmentPlan("select greatest(d1, d2) from tsig"), "greatest(4: d1, 5: d2)");
        assertContains(getFragmentPlan("select least(d1, d2) from tsig"), "least(4: d1, 5: d2)");
    }
}
