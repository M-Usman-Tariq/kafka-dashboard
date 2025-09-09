package com.projects.kafkadash.util;

import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

@Getter
@Setter
public class OffsetKey {
    String subscriberGroupName;
    String topicName;
    int partition;

    private static final Logger logger = LoggerFactory.getLogger(OffsetKey.class);

    public static OffsetKey tryParse(byte[] serializedKey) {
        try {
            ByteBuffer buffer = ByteBuffer.wrap(serializedKey);
            short version = buffer.getShort();

            if(version == 0 || version == 1){
                var offsetKey = new OffsetKey();
                offsetKey.setSubscriberGroupName(readString(buffer));
                offsetKey.setTopicName(readString(buffer));
                offsetKey.setPartition(buffer.getInt());
                return offsetKey;
            }
        }catch (Exception e){
            logger.error(e.getMessage());
        }

        return null;
    }

    private static String readString(ByteBuffer buffer) {
        short length = buffer.getShort();
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes);
    }
}
