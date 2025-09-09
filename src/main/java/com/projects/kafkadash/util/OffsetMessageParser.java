package com.projects.kafkadash.util;

import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;

public class OffsetMessageParser {

    private static final Schema OFFSET_COMMIT_VALUE_SCHEMA_V0 = new Schema(
            new Field("offset", Type.INT64),
            new Field("metadata", Type.STRING),
            new Field("commit_timestamp", Type.INT64)
    );

    private static final Schema OFFSET_COMMIT_VALUE_SCHEMA_V1 = new Schema(
            new Field("offset", Type.INT64),
            new Field("metadata", Type.NULLABLE_STRING),
            new Field("commit_timestamp", Type.INT64)
    );

    private static final Schema OFFSET_COMMIT_VALUE_SCHEMA_V2 = new Schema(
            new Field("offset", Type.INT64),
            new Field("metadata", Type.NULLABLE_STRING),
            new Field("commit_timestamp", Type.INT64),
            new Field("expire_timestamp", Type.INT64)
    );

    private static final Schema OFFSET_COMMIT_VALUE_SCHEMA_V3 = new Schema(
            new Field("offset", Type.INT64),
            new Field("leader_epoch", Type.INT32),
            new Field("metadata", Type.NULLABLE_STRING),
            new Field("commit_timestamp", Type.INT64)
    );

    public static OffsetAndMetadata parseOffsetMessageValue(byte[] value) {
        if (value == null) return null;
        ByteBuffer buffer = ByteBuffer.wrap(value);

        short version = buffer.getShort();
        Schema schema;
        switch (version) {
            case 0 -> schema = OFFSET_COMMIT_VALUE_SCHEMA_V0;
            case 1 -> schema = OFFSET_COMMIT_VALUE_SCHEMA_V1;
            case 2 -> schema = OFFSET_COMMIT_VALUE_SCHEMA_V2;
            case 3 -> schema = OFFSET_COMMIT_VALUE_SCHEMA_V3;
            default -> throw new IllegalArgumentException("Unknown offset message version: " + version);
        }

        Struct struct = schema.read(buffer);
        long offset = struct.getLong("offset");
        String metadata = null;
        if (struct.hasField("metadata"))
            metadata = (String) struct.get("metadata");
        long commitTs = struct.getLong("commit_timestamp");

        return new OffsetAndMetadata(offset, metadata, commitTs);
    }

    public record OffsetAndMetadata(long offset, String metadata, long commitTimestamp) {}
}

