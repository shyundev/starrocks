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
import com.starrocks.mysql.MysqlCodec;
import com.starrocks.sql.ast.expression.DateLiteral;
import com.starrocks.sql.ast.expression.DecimalLiteral;
import com.starrocks.sql.ast.expression.FloatLiteral;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.LargeIntLiteral;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.ast.expression.LiteralExprFactory;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.optimizer.validate.ValidateException;
import com.starrocks.type.DateType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.StringType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
/**
 * Utilities for parsing literal values out of the MySQL binary protocol payload.
 * Used by {@link com.starrocks.qe.ConnectProcessor} when binding prepared statement parameters.
 */
public final class MysqlParamParser {
    private MysqlParamParser() {
    }

    public static LiteralExpr createLiteral(int mysqlTypeCode, ByteBuffer data) throws AnalysisException {
        // the type is followed by a flags byte, of which only UNSIGNED_FLAG affects how the value is read
        boolean unsigned = (mysqlTypeCode & 0x8000) != 0;
        switch (mysqlTypeCode & 0xff) {
            case 0: // MYSQL_TYPE_DECIMAL
                return new DecimalLiteral(readLengthEncodedString(data));
            case 1: // MYSQL_TYPE_TINY
                return createIntLiteral(readIntegerValue(PrimitiveType.TINYINT, data, unsigned), IntegerType.TINYINT,
                        unsigned);
            case 2: // MYSQL_TYPE_SHORT
                return createIntLiteral(readIntegerValue(PrimitiveType.SMALLINT, data, unsigned),
                        IntegerType.SMALLINT, unsigned);
            case 3: // MYSQL_TYPE_LONG
                return createIntLiteral(readIntegerValue(PrimitiveType.INT, data, unsigned), IntegerType.INT,
                        unsigned);
            case 4: // MYSQL_TYPE_FLOAT
                return new FloatLiteral((double) data.getFloat(), FloatType.FLOAT);
            case 5: // MYSQL_TYPE_DOUBLE
                return new FloatLiteral(data.getDouble(), FloatType.DOUBLE);
            case 7: // MYSQL_TYPE_TIMESTAMP
            case 12: // MYSQL_TYPE_DATETIME
            case 17: // MYSQL_TYPE_TIMESTAMP2
                return createDatetimeLiteral(DateType.DATETIME, data);
            case 8: // MYSQL_TYPE_LONGLONG
                long value = readIntegerValue(PrimitiveType.BIGINT, data, unsigned);
                if (unsigned && value < 0) {
                    return new LargeIntLiteral(Long.toUnsignedString(value));
                }
                return new IntLiteral(value, IntegerType.BIGINT);
            case 10: // MYSQL_TYPE_DATE
                return createDateLiteral(DateType.DATE, data);
            case 15: // MYSQL_TYPE_VARCHAR
                return LiteralExprFactory.create(readLengthEncodedString(data), VarcharType.VARCHAR);
            case 253: // MYSQL_TYPE_STRING
            case 254: // MYSQL_TYPE_STRING
                return LiteralExprFactory.create(readLengthEncodedString(data), StringType.STRING);
            default:
                throw new AnalysisException("unknown mysql type code " + mysqlTypeCode);
        }
    }

    private static LiteralExpr createIntLiteral(long value, Type type, boolean unsigned) {
        // an unsigned value may not fit the signed type of the same width, so pick the type by value
        return unsigned ? new IntLiteral(value) : new IntLiteral(value, type);
    }

    private static long readIntegerValue(PrimitiveType primitiveType, ByteBuffer data, boolean unsigned) {
        switch (primitiveType) {
            case BOOLEAN:
            case TINYINT:
                return unsigned ? Byte.toUnsignedInt(data.get()) : data.get();
            case SMALLINT:
                return unsigned ? data.getChar() : data.getShort();
            case INT:
                return unsigned ? Integer.toUnsignedLong(data.getInt()) : data.getInt();
            case BIGINT:
                return data.getLong();
            default:
                throw new ValidateException("Unsupported integer type: " + primitiveType, ErrorType.INTERNAL_ERROR);
        }
    }

    private static LiteralExpr createDateLiteral(Type targetType, ByteBuffer data) throws AnalysisException {
        int len = getParamLength(data);
        if (len < 4) {
            return DateLiteral.createMinValue(targetType);
        }
        int year = data.getChar();
        int month = Byte.toUnsignedInt(data.get());
        int day = Byte.toUnsignedInt(data.get());
        return new DateLiteral(year, month, day);
    }

    private static LiteralExpr createDatetimeLiteral(Type targetType, ByteBuffer data) throws AnalysisException {
        int len = getParamLength(data);
        if (len < 4) {
            return DateLiteral.createMinValue(targetType);
        }
        int year = data.getChar();
        int month = Byte.toUnsignedInt(data.get());
        int day = Byte.toUnsignedInt(data.get());
        int hour = 0;
        int minute = 0;
        int second = 0;
        if (len > 4) {
            hour = Byte.toUnsignedInt(data.get());
            minute = Byte.toUnsignedInt(data.get());
            second = Byte.toUnsignedInt(data.get());
        }
        int microsecond = 0;
        if (len > 7) {
            microsecond = data.getInt();
        }
        return new DateLiteral(year, month, day, hour, minute, second, microsecond);
    }

    private static String readLengthEncodedString(ByteBuffer data) {
        int len = getParamLength(data);
        len = Math.min(len, data.remaining());
        byte[] bytes = new byte[len];
        data.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * Port from MySQL get_param_length implementation.
     */
    public static int getParamLength(ByteBuffer data) {
        int maxLen = data.remaining();
        if (maxLen < 1) {
            return 0;
        }
        // get and advance 1 byte
        int len = MysqlCodec.readInt1(data);
        if (len == 252) {
            if (maxLen < 3) {
                return 0;
            }
            // get and advance 2 bytes
            return MysqlCodec.readInt2(data);
        } else if (len == 253) {
            if (maxLen < 4) {
                return 0;
            }
            // get and advance 3 bytes
            return MysqlCodec.readInt3(data);
        } else if (len == 254) {
            /*
            In our client-server protocol all numbers bigger than 2^24
            stored as 8 bytes with uint8korr. Here we always know that
            parameter length is less than 2^4 so we don't look at the second
            4 bytes. But still we need to obey the protocol hence 9 in the
            assignment below.
            */
            if (maxLen < 9) {
                return 0;
            }
            len = MysqlCodec.readInt4(data);
            MysqlCodec.readFixedString(data, 4);
            return len;
        } else if (len == 255) {
            return 0;
        } else {
            return len;
        }
    }
}
