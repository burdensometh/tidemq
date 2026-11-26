package tidemq.wire;

import tidemq.ingress.BatchReceipt;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

/**
 * Adapts small request records/ builders whose exact type name may differ between
 * call sites. The strongly typed TideClient overloads are preferred; this only
 * supports one-argument batch request objects.
 */
final class BatchRequests {
    private BatchRequests() {}

    static BatchReceipt send(TideClient client, Object request) throws IOException {
        if (request == null) {
            throw new IllegalArgumentException("batch request is null");
        }
        if (request instanceof BatchReceipt) {
            throw new IllegalArgumentException("batch receipt is not a produce request");
        }
        String producer = string(request, "producer", "producerId", "producerName");
        String topic = string(request, "topic", "topicName");
        Integer partition = integer(request, "partition", "partitionId", "partitionNumber", "partitionIndex");
        Object batchId = invoke(request, "batchId", "id", "sequence", "batchSequence", "batchKey");
        Object items = invoke(request, "items", "messages", "records", "entries",
                "batchItems", "payloads", "values");
        if (producer == null || topic == null || partition == null || batchId == null || items == null) {
            throw new IllegalArgumentException("cannot read producer batch request of type "
                    + request.getClass().getName());
        }
        List<?> list = asList(items);
        if (batchId instanceof Number number) {
            return client.produceBatch(producer, number.longValue(), topic, partition, list);
        }
        if (batchId instanceof String id) {
            return client.produceBatch(producer, TideClient.stringBatchSurrogate(id), topic, partition, list);
        }
        throw new IllegalArgumentException("unsupported batch id type " + batchId.getClass());
    }

    private static List<?> asList(Object items) {
        if (items instanceof List<?> list) {
            return list;
        }
        if (items instanceof Object[] array) {
            return Arrays.asList(array);
        }
        if (items instanceof byte[] bytes) {
            return List.of(bytes);
        }
        throw new IllegalArgumentException("batch items must be a list or array");
    }

    private static String string(Object target, String... names) {
        Object value = invoke(target, names);
        return value instanceof String text ? text : null;
    }

    private static Integer integer(Object target, String... names) {
        Object value = invoke(target, names);
        if (value instanceof Number number) {
            return number.intValue();
        }
        return null;
    }

    private static Object invoke(Object target, String... names) {
        Class<?> type = target.getClass();
        for (String name : names) {
            try {
                Method method = type.getMethod(name);
                return method.invoke(target);
            } catch (ReflectiveOperationException ignored) {
                // Try record-style and getter-style spellings.
            }
            try {
                Method method = type.getMethod("get" + Character.toUpperCase(name.charAt(0)) + name.substring(1));
                return method.invoke(target);
            } catch (ReflectiveOperationException ignored) {
                // Continue.
            }
        }
        return null;
    }
}
