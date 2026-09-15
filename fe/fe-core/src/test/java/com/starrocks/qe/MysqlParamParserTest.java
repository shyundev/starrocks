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

import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.LargeIntLiteral;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.type.PrimitiveType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class MysqlParamParserTest {
    private static final int MYSQL_TYPE_TINY = 1;
    private static final int MYSQL_TYPE_SHORT = 2;
    private static final int MYSQL_TYPE_LONG = 3;
    private static final int MYSQL_TYPE_LONGLONG = 8;
    // the flags byte follows the type byte in COM_STMT_EXECUTE, so it lands in the high byte
    private static final int UNSIGNED_FLAG = 0x8000;

    private static ByteBuffer packet(int... bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(bytes.length).order(ByteOrder.LITTLE_ENDIAN);
        for (int b : bytes) {
            buffer.put((byte) b);
        }
        buffer.flip();
        return buffer;
    }

    private static long intValue(int typeCode, ByteBuffer data) throws Exception {
        LiteralExpr literal = MysqlParamParser.createLiteral(typeCode, data);
        Assertions.assertTrue(literal instanceof IntLiteral, literal.getClass().getName());
        Assertions.assertFalse(data.hasRemaining());
        return ((IntLiteral) literal).getLongValue();
    }

    @Test
    public void testNegativeShort() throws Exception {
        LiteralExpr literal = MysqlParamParser.createLiteral(MYSQL_TYPE_SHORT, packet(0x18, 0xfc));
        Assertions.assertEquals(-1000, ((IntLiteral) literal).getLongValue());
        Assertions.assertEquals(PrimitiveType.SMALLINT, literal.getType().getPrimitiveType());
        Assertions.assertEquals(-32768,
                intValue(MYSQL_TYPE_SHORT, packet(0x00, 0x80)));
        Assertions.assertEquals(-1,
                intValue(MYSQL_TYPE_SHORT, packet(0xff, 0xff)));
    }

    @Test
    public void testUnsignedFlag() throws Exception {
        Assertions.assertEquals(5, intValue(MYSQL_TYPE_TINY | UNSIGNED_FLAG, packet(0x05)));
        Assertions.assertEquals(200, intValue(MYSQL_TYPE_TINY | UNSIGNED_FLAG, packet(0xc8)));
        Assertions.assertEquals(60000, intValue(MYSQL_TYPE_SHORT | UNSIGNED_FLAG, packet(0x60, 0xea)));
        Assertions.assertEquals(4000000000L,
                intValue(MYSQL_TYPE_LONG | UNSIGNED_FLAG, packet(0x00, 0x28, 0x6b, 0xee)));
        Assertions.assertEquals(Long.MAX_VALUE,
                intValue(MYSQL_TYPE_LONGLONG | UNSIGNED_FLAG,
                        packet(0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f)));

        LiteralExpr literal = MysqlParamParser.createLiteral(MYSQL_TYPE_LONGLONG | UNSIGNED_FLAG,
                packet(0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff));
        Assertions.assertTrue(literal instanceof LargeIntLiteral, literal.getClass().getName());
        Assertions.assertEquals("18446744073709551615", literal.getStringValue());
    }

    @Test
    public void testSignedIntegersKeepDriverType() throws Exception {
        LiteralExpr tiny = MysqlParamParser.createLiteral(MYSQL_TYPE_TINY, packet(0xfb));
        Assertions.assertEquals(-5, ((IntLiteral) tiny).getLongValue());
        Assertions.assertEquals(PrimitiveType.TINYINT, tiny.getType().getPrimitiveType());
        LiteralExpr big = MysqlParamParser.createLiteral(MYSQL_TYPE_LONGLONG,
                packet(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80));
        Assertions.assertEquals(Long.MIN_VALUE, ((IntLiteral) big).getLongValue());
        Assertions.assertEquals(PrimitiveType.BIGINT, big.getType().getPrimitiveType());
    }
}
