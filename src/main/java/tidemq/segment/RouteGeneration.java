package tidemq.segment;

import tidemq.Binary;
import tidemq.Boundary;
import tidemq.BrokerException;
import tidemq.Settings;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Persistent routing generations for one topic. Generation zero records the
 * partition count the topic was created with. Every later generation is an
 * online expansion and carries the end offset each already existing partition
 * had at the moment the new partitions became routable. Those boundaries stay
 * fixed forever and are reloaded verbatim after a restart, so automatic key
 * routing keeps an unambiguous old-route/new-route split.
 */
public record RouteGeneration(int partitionCount, List<Long> boundaries) {
    public RouteGeneration {
        if (partitionCount < 1 || partitionCount > Settings.MAX_PARTITIONS) {
            throw new IllegalArgumentException("invalid routed partition count");
        }
        boundaries = List.copyOf(boundaries);
        for (long boundary : boundaries) {
            if (boundary < 0) {
                throw new IllegalArgumentException("routing boundary must be nonnegative");
            }
        }
    }

    public boolean isExpansion() {
        return !boundaries.isEmpty();
    }

    static void write(DataOutputStream out, RouteGeneration generation) throws IOException {
        out.writeInt(generation.partitionCount());
        out.writeInt(generation.boundaries().size());
        for (long boundary : generation.boundaries()) {
            out.writeLong(boundary);
        }
    }

    static RouteGeneration read(DataInputStream in) throws IOException {
        int partitions = in.readInt();
        if (partitions < 1 || partitions > Settings.MAX_PARTITIONS) {
            throw new IOException("invalid persisted partition count");
        }
        int boundaryCount = Binary.readCount(in, Settings.MAX_PARTITIONS);
        var boundaries = new ArrayList<Long>(boundaryCount);
        for (int i = 0; i < boundaryCount; i++) {
            long boundary = in.readLong();
            if (boundary < 0) {
                throw new IOException("negative persisted routing boundary");
            }
            boundaries.add(boundary);
        }
        return new RouteGeneration(partitions, List.copyOf(boundaries));
    }
}
