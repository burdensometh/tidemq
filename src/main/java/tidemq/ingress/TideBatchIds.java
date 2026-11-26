package tidemq.ingress;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** Stable non-negative surrogate for textual batch identifiers on the v1 long-id path. */
public final class TideBatchIds {
    private TideBatchIds() {}

    public static long surrogate(String batchId) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(batchId.getBytes(StandardCharsets.UTF_8));
            long value = 0;
            for (int i = 0; i < 8; i++) {
                value = (value << 8) | (digest[i] & 255L);
            }
            return value & Long.MAX_VALUE;
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("Java runtime is missing SHA-256", impossible);
        }
    }
}
