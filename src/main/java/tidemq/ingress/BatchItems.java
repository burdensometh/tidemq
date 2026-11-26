package tidemq.ingress;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/** Converts common byte-holder item records into {@link BatchItem}. */
public final class BatchItems {
    private BatchItems() {}

    public static BatchItem from(Object value) {
        if (value == null) {
            throw new IllegalArgumentException("batch item is null");
        }
        if (value instanceof BatchItem item) {
            return item;
        }
        if (value instanceof ProduceItem item) {
            return new BatchItem(item.key(), item.payload());
        }
        if (value instanceof byte[] payload) {
            return new BatchItem(new byte[0], payload);
        }
        if (value instanceof String text) {
            return new BatchItem(new byte[0], text.getBytes(StandardCharsets.UTF_8));
        }
        if (value instanceof ByteBuffer buffer) {
            ByteBuffer duplicate = buffer.duplicate();
            byte[] payload = new byte[duplicate.remaining()];
            duplicate.get(payload);
            return new BatchItem(new byte[0], payload);
        }
        if (value instanceof Map<?, ?> map) {
            Object key = map.get("key");
            Object payload = map.containsKey("payload") ? map.get("payload") : map.get("value");
            if (key instanceof byte[] keyBytes && payload instanceof byte[] payloadBytes) {
                return new BatchItem(keyBytes, payloadBytes);
            }
        }
        try {
            byte[] key = invokeBytes(value, "key", "getKey", "keyBytes");
            byte[] payload = invokeBytes(value, "payload", "getPayload", "value", "getValue",
                    "body", "getBody", "data", "getData", "content", "getContent", "bytes");
            if (payload != null) {
                return new BatchItem(key == null ? new byte[0] : key, payload);
            }
        } catch (ReflectiveOperationException ignored) {
            // Fall through.
        }
        throw new IllegalArgumentException("unsupported batch item type: " + value.getClass().getName());
    }

    private static byte[] invokeBytes(Object target, String... names) throws ReflectiveOperationException {
        Class<?> type = target.getClass();
        for (String name : names) {
            try {
                Method method = type.getMethod(name);
                Object result = method.invoke(target);
                if (result instanceof byte[] bytes) {
                    return bytes;
                }
            } catch (NoSuchMethodException ignored) {
                // Try the next accessor spelling.
            }
        }
        return null;
    }
}
