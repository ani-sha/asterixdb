package org.apache.hyracks.algebricks.runtime.util;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class DynamicFilterSerializer {

    public static final byte TAG_INT32 = 0x03;  // INTEGER ordinal
    public static final byte TAG_INT64 = 0x04;  // BIGINT ordinal
    public static final byte TAG_STRING = 0x0D; // STRING ordinal

    public static byte[] serialize(String value, String type) {
        switch (type) {
            case "int":
                return serializeInt32(Integer.parseInt(value.trim()));
            case "bigint":
                return serializeInt64(Long.parseLong(value.trim()));
            case "string":
                return serializeString(value.trim());
            default:
                throw new IllegalArgumentException("Unsupported type for dynamic serialization: " + type);
        }
    }

    public static byte[] serializeInt32(int value) {
        byte[] bytes = new byte[1 + 4]; // 1 for tag, 4 for int
        bytes[0] = TAG_INT32;
        ByteBuffer.wrap(bytes, 1, 4).putInt(value);
        return bytes;
    }

    public static byte[] serializeInt64(long value) {
        byte[] bytes = new byte[1 + 8]; // 1 for tag, 8 for long
        bytes[0] = TAG_INT64;
        ByteBuffer.wrap(bytes, 1, 8).putLong(value);
        return bytes;
    }

    public static byte[] serializeString(String value) {
        byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
        byte[] bytes = new byte[1 + 4 + utf8.length]; // 1 tag + 4 length + utf8 bytes
        bytes[0] = TAG_STRING;
        ByteBuffer.wrap(bytes, 1, 4).putInt(utf8.length);
        System.arraycopy(utf8, 0, bytes, 5, utf8.length);
        return bytes;
    }
}

