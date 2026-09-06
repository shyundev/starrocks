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

package com.starrocks.qe;

import com.starrocks.common.AnalysisException;
import com.starrocks.sql.ast.expression.DecimalLiteral;
import com.starrocks.sql.ast.expression.LiteralExpr;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class MysqlParamParserTest {
    private static final int MYSQL_TYPE_DECIMAL = 0;
    private static final int MYSQL_TYPE_LONG = 3;
    private static final int MYSQL_TYPE_NEWDECIMAL = 246;
    private static final int UNKNOWN_TYPE_CODE = 255;

    private static ByteBuffer packet(int... bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(bytes.length).order(ByteOrder.LITTLE_ENDIAN);
        for (int b : bytes) {
            buffer.put((byte) b);
        }
        buffer.flip();
        return buffer;
    }

    @Test
    public void testDecimalTypeCodes() throws Exception {
        for (int typeCode : new int[] {MYSQL_TYPE_DECIMAL, MYSQL_TYPE_NEWDECIMAL}) {
            LiteralExpr literal = MysqlParamParser.createLiteral(typeCode, packet(0x05, '-', '1', '.', '2', '5'));
            Assertions.assertTrue(literal instanceof DecimalLiteral, literal.getClass().getName());
            Assertions.assertEquals("-1.25", literal.getStringValue());
        }
    }

    @Test
    public void testDecimalLeavesTheCursorOnTheNextParameter() throws Exception {
        ByteBuffer buffer = packet(0x05, '-', '1', '.', '2', '5', 0x2A, 0x00, 0x00, 0x00);

        LiteralExpr decimal = MysqlParamParser.createLiteral(MYSQL_TYPE_NEWDECIMAL, buffer);
        Assertions.assertEquals("-1.25", decimal.getStringValue());

        LiteralExpr next = MysqlParamParser.createLiteral(MYSQL_TYPE_LONG, buffer);
        Assertions.assertEquals("42", next.getStringValue());
        Assertions.assertEquals(0, buffer.remaining());
    }

    @Test
    public void testUnknownTypeCode() {
        Assertions.assertThrows(AnalysisException.class,
                () -> MysqlParamParser.createLiteral(UNKNOWN_TYPE_CODE, packet(0x01, '1')));
    }
}
