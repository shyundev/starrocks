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

package com.starrocks.connector.hive;

import com.starrocks.common.Config;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ConnectorTableMetadataProcessorTest {

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
    }

    @Test
    public void testIntervalFollowsConfig() {
        int originInterval = Config.background_refresh_metadata_interval_millis;
        try {
            ConnectorTableMetadataProcessor processor = new ConnectorTableMetadataProcessor();
            Assertions.assertEquals(originInterval, processor.getInterval());

            Config.background_refresh_metadata_interval_millis = originInterval + 1000;
            processor.runAfterCatalogReady();
            Assertions.assertEquals(originInterval + 1000, processor.getInterval());
        } finally {
            Config.background_refresh_metadata_interval_millis = originInterval;
        }
    }
}
