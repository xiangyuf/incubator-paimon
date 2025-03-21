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

package org.apache.paimon.utils;

import org.apache.paimon.data.BinaryArray;
import org.apache.paimon.data.BinaryMap;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.DataGetters;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.NestedRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Utils for {@link InternalRow} structures. */
public class InternalRowUtils {

    public static boolean equals(Object data1, Object data2, DataType dataType) {
        if ((data1 == null) != (data2 == null)) {
            return false;
        }
        if (data1 != null) {
            if (data1 instanceof InternalRow) {
                RowType rowType = (RowType) dataType;
                int len = rowType.getFieldCount();
                for (int i = 0; i < len; i++) {
                    Object value1 = get((InternalRow) data1, i, rowType.getTypeAt(i));
                    Object value2 = get((InternalRow) data2, i, rowType.getTypeAt(i));
                    if (!equals(value1, value2, rowType.getTypeAt(i))) {
                        return false;
                    }
                }
            } else if (data1 instanceof InternalArray) {
                if (((InternalArray) data1).size() != ((InternalArray) data2).size()) {
                    return false;
                }
                ArrayType arrayType = (ArrayType) dataType;
                for (int i = 0; i < ((InternalArray) data1).size(); i++) {
                    Object value1 = get((InternalArray) data1, i, arrayType.getElementType());
                    Object value2 = get((InternalArray) data2, i, arrayType.getElementType());
                    if (!equals(value1, value2, arrayType.getElementType())) {
                        return false;
                    }
                }
            } else if (data1 instanceof InternalMap) {
                if (((InternalMap) data1).size() != ((InternalMap) data2).size()) {
                    return false;
                }
                MapType mapType = (MapType) dataType;
                GenericMap map1;
                GenericMap map2;
                if (data1 instanceof GenericMap) {
                    map1 = (GenericMap) data1;
                    map2 = (GenericMap) data2;
                } else {
                    map1 =
                            copyToGenericMap(
                                    (InternalMap) data1,
                                    mapType.getKeyType(),
                                    mapType.getValueType());
                    map2 =
                            copyToGenericMap(
                                    (InternalMap) data2,
                                    mapType.getKeyType(),
                                    mapType.getValueType());
                }
                InternalArray keyArray1 = map1.keyArray();
                for (int i = 0; i < map1.size(); i++) {
                    Object key = get(keyArray1, i, mapType.getKeyType());
                    if (!map2.contains(key)
                            || !equals(map1.get(key), map2.get(key), mapType.getValueType())) {
                        return false;
                    }
                }
            } else if (data1 instanceof byte[]) {
                if (!java.util.Arrays.equals((byte[]) data1, (byte[]) data2)) {
                    return false;
                }
            } else if (data1 instanceof Float && java.lang.Float.isNaN((Float) data1)) {
                if (!java.lang.Float.isNaN((Float) data2)) {
                    return false;
                }
            } else if (data1 instanceof Double && java.lang.Double.isNaN((Double) data1)) {
                if (!java.lang.Double.isNaN((Double) data2)) {
                    return false;
                }
            } else {
                if (!data1.equals(data2)) {
                    return false;
                }
            }
        }
        return true;
    }

    public static int hash(Object data, DataType dataType) {
        if (data == null) {
            return 0;
        }
        int result = 0;
        if (data instanceof InternalRow) {
            RowType rowType = (RowType) dataType;
            int len = rowType.getFieldCount();
            for (int i = 0; i < len; i++) {
                Object v = get((InternalRow) data, i, rowType.getTypeAt(i));
                result = 37 * result + hash(v, rowType.getTypeAt(i));
            }
        } else if (data instanceof InternalArray) {
            ArrayType arrayType = (ArrayType) dataType;
            int len = ((InternalArray) data).size();
            for (int i = 0; i < len; i++) {
                Object v = get((InternalArray) data, i, arrayType.getElementType());
                result = 37 * result + hash(v, arrayType.getElementType());
            }
        } else if (data instanceof InternalMap) {
            MapType mapType = (MapType) dataType;
            GenericMap map;
            if (data instanceof GenericMap) {
                map = (GenericMap) data;
            } else {
                map =
                        copyToGenericMap(
                                (InternalMap) data, mapType.getKeyType(), mapType.getValueType());
            }
            InternalArray keyArray = map.keyArray();
            for (int i = 0; i < map.size(); i++) {
                Object key = get(keyArray, i, mapType.getKeyType());
                result = 37 * result + hash(key, mapType.getKeyType());
                result = 37 * result + hash(map.get(key), mapType.getValueType());
            }
        } else if (data instanceof byte[]) {
            result = Arrays.hashCode((byte[]) data);
        } else {
            result = data.hashCode();
        }
        return result;
    }

    public static InternalRow copyInternalRow(InternalRow row, RowType rowType) {
        if (row instanceof BinaryRow) {
            return ((BinaryRow) row).copy();
        } else if (row instanceof NestedRow) {
            return ((NestedRow) row).copy();
        } else {
            GenericRow ret = new GenericRow(row.getFieldCount());
            ret.setRowKind(row.getRowKind());

            for (int i = 0; i < row.getFieldCount(); ++i) {
                DataType fieldType = rowType.getTypeAt(i);
                ret.setField(i, copy(get(row, i, fieldType), fieldType));
            }

            return ret;
        }
    }

    public static InternalArray copyArray(InternalArray from, DataType eleType) {
        if (from instanceof BinaryArray) {
            return ((BinaryArray) from).copy();
        }

        if (!eleType.isNullable()) {
            switch (eleType.getTypeRoot()) {
                case BOOLEAN:
                    return new GenericArray(from.toBooleanArray());
                case TINYINT:
                    return new GenericArray(from.toByteArray());
                case SMALLINT:
                    return new GenericArray(from.toShortArray());
                case INTEGER:
                case DATE:
                case TIME_WITHOUT_TIME_ZONE:
                    return new GenericArray(from.toIntArray());
                case BIGINT:
                    return new GenericArray(from.toLongArray());
                case FLOAT:
                    return new GenericArray(from.toFloatArray());
                case DOUBLE:
                    return new GenericArray(from.toDoubleArray());
            }
        }

        Object[] newArray = new Object[from.size()];

        for (int i = 0; i < newArray.length; ++i) {
            if (!from.isNullAt(i)) {
                newArray[i] = copy(get(from, i, eleType), eleType);
            } else {
                newArray[i] = null;
            }
        }

        return new GenericArray(newArray);
    }

    private static InternalMap copyMap(InternalMap map, DataType keyType, DataType valueType) {
        if (map instanceof BinaryMap) {
            return ((BinaryMap) map).copy();
        }

        return copyToGenericMap(map, keyType, valueType);
    }

    private static GenericMap copyToGenericMap(
            InternalMap map, DataType keyType, DataType valueType) {
        Map<Object, Object> javaMap = new HashMap<>();
        InternalArray keys = map.keyArray();
        InternalArray values = map.valueArray();
        for (int i = 0; i < keys.size(); i++) {
            javaMap.put(
                    copy(get(keys, i, keyType), keyType),
                    copy(get(values, i, valueType), valueType));
        }
        return new GenericMap(javaMap);
    }

    public static Object copy(Object o, DataType type) {
        if (o instanceof BinaryString) {
            return ((BinaryString) o).copy();
        } else if (o instanceof InternalRow) {
            return copyInternalRow((InternalRow) o, (RowType) type);
        } else if (o instanceof InternalArray) {
            return copyArray((InternalArray) o, ((ArrayType) type).getElementType());
        } else if (o instanceof InternalMap) {
            if (type instanceof MapType) {
                return copyMap(
                        (InternalMap) o,
                        ((MapType) type).getKeyType(),
                        ((MapType) type).getValueType());
            } else {
                return copyMap(
                        (InternalMap) o, ((MultisetType) type).getElementType(), new IntType());
            }
        } else if (o instanceof byte[]) {
            byte[] copy = new byte[((byte[]) o).length];
            System.arraycopy(((byte[]) o), 0, copy, 0, ((byte[]) o).length);
            return copy;
        } else if (o instanceof Decimal) {
            return ((Decimal) o).copy();
        }
        return o;
    }

    public static Object get(DataGetters dataGetters, int pos, DataType fieldType) {
        if (dataGetters.isNullAt(pos)) {
            return null;
        }
        switch (fieldType.getTypeRoot()) {
            case BOOLEAN:
                return dataGetters.getBoolean(pos);
            case TINYINT:
                return dataGetters.getByte(pos);
            case SMALLINT:
                return dataGetters.getShort(pos);
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return dataGetters.getInt(pos);
            case BIGINT:
                return dataGetters.getLong(pos);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampType timestampType = (TimestampType) fieldType;
                return dataGetters.getTimestamp(pos, timestampType.getPrecision());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                LocalZonedTimestampType lzTs = (LocalZonedTimestampType) fieldType;
                return dataGetters.getTimestamp(pos, lzTs.getPrecision());
            case FLOAT:
                return dataGetters.getFloat(pos);
            case DOUBLE:
                return dataGetters.getDouble(pos);
            case CHAR:
            case VARCHAR:
                return dataGetters.getString(pos);
            case DECIMAL:
                DecimalType decimalType = (DecimalType) fieldType;
                return dataGetters.getDecimal(
                        pos, decimalType.getPrecision(), decimalType.getScale());
            case ARRAY:
                return dataGetters.getArray(pos);
            case MAP:
            case MULTISET:
                return dataGetters.getMap(pos);
            case ROW:
                return dataGetters.getRow(pos, ((RowType) fieldType).getFieldCount());
            case BINARY:
            case VARBINARY:
                return dataGetters.getBinary(pos);
            case VARIANT:
                return dataGetters.getVariant(pos);
            default:
                throw new UnsupportedOperationException("Unsupported type: " + fieldType);
        }
    }

    public static InternalArray toStringArrayData(@Nullable List<String> list) {
        if (list == null) {
            return null;
        }

        return new GenericArray(list.stream().map(BinaryString::fromString).toArray());
    }

    public static List<String> fromStringArrayData(InternalArray arrayData) {
        List<String> list = new ArrayList<>(arrayData.size());
        for (int i = 0; i < arrayData.size(); i++) {
            list.add(arrayData.isNullAt(i) ? null : arrayData.getString(i).toString());
        }
        return list;
    }

    public static long castToIntegral(Decimal dec) {
        BigDecimal bd = dec.toBigDecimal();
        // rounding down. This is consistent with float=>int,
        // and consistent with SQLServer, Spark.
        bd = bd.setScale(0, RoundingMode.DOWN);
        return bd.longValue();
    }

    public static InternalRow.FieldGetter[] createFieldGetters(List<DataType> fieldTypes) {
        InternalRow.FieldGetter[] fieldGetters = new InternalRow.FieldGetter[fieldTypes.size()];
        for (int i = 0; i < fieldTypes.size(); i++) {
            fieldGetters[i] = createNullCheckingFieldGetter(fieldTypes.get(i), i);
        }
        return fieldGetters;
    }

    public static InternalRow.FieldGetter createNullCheckingFieldGetter(
            DataType dataType, int index) {
        InternalRow.FieldGetter getter = InternalRow.createFieldGetter(dataType, index);
        if (dataType.isNullable()) {
            return getter;
        } else {
            return row -> {
                if (row.isNullAt(index)) {
                    return null;
                }
                return getter.getFieldOrNull(row);
            };
        }
    }

    public static int compare(Object x, Object y, DataTypeRoot type) {
        int ret;
        switch (type) {
            case DECIMAL:
                Decimal xDD = (Decimal) x;
                Decimal yDD = (Decimal) y;
                ret = xDD.compareTo(yDD);
                break;
            case TINYINT:
                ret = Byte.compare((byte) x, (byte) y);
                break;
            case SMALLINT:
                ret = Short.compare((short) x, (short) y);
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                ret = Integer.compare((int) x, (int) y);
                break;
            case BIGINT:
                ret = Long.compare((long) x, (long) y);
                break;
            case FLOAT:
                ret = Float.compare((float) x, (float) y);
                break;
            case DOUBLE:
                ret = Double.compare((double) x, (double) y);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                Timestamp xDD1 = (Timestamp) x;
                Timestamp yDD1 = (Timestamp) y;
                ret = xDD1.compareTo(yDD1);
                break;
            case BINARY:
            case VARBINARY:
                ret = byteArrayCompare((byte[]) x, (byte[]) y);
                break;
            case VARCHAR:
            case CHAR:
                ret = ((BinaryString) x).compareTo((BinaryString) y);
                break;
            default:
                throw new IllegalArgumentException("Incomparable type: " + type);
        }
        return ret;
    }

    private static int byteArrayCompare(byte[] array1, byte[] array2) {
        for (int i = 0, j = 0; i < array1.length && j < array2.length; i++, j++) {
            int a = (array1[i] & 0xff);
            int b = (array2[j] & 0xff);
            if (a != b) {
                return a - b;
            }
        }
        return array1.length - array2.length;
    }
}
