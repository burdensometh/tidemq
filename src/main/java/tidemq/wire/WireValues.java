package tidemq.wire;

import tidemq.Binary;
import tidemq.BrokerException;
import tidemq.Settings;
import tidemq.deadletter.DeadLetterReceipt;
import tidemq.deadletter.DeadLetters;
import tidemq.delivery.DeliveryLease;
import tidemq.delivery.FlowView;
import tidemq.group.AckFrontier;
import tidemq.group.GroupDirectory;
import tidemq.group.GroupEpoch;
import tidemq.ingress.ProduceReceipt;
import tidemq.segment.MessageV1;
import tidemq.segment.RetentionPreview;
import tidemq.segment.Segment;
import tidemq.segment.SegmentStore;
import tidemq.segment.TopicPartition;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

final class WireValues {
    static final int VERSION = 1;
    static final int MAX_TOPICS = 16;
    static final int MAX_PARTITIONS = Settings.MAX_PARTITIONS;
    static final int MAX_BATCH = 256;
    static final int MAX_SEGMENTS = 128;
    static final int MAX_MEMBERS = 256;
    static final int MAX_ERROR_CHARS = 192;

    record Hello(int minVersion, int maxVersion, int maxFrame) {}
    record Welcome(int version, int maxFrame, int maxBatch) {}
    record Publication(String producer, long sequence, String topic, int partition, byte[] key, byte[] value) {}
    record Join(String group, String member, Set<String> topics) {}
    record Pull(long epoch, int limit) {}
    record Settlement(long epoch, List<DeliveryLease.Ref> refs) {}
    record PartitionQuery(TopicPartition partition, int limit) {}
    record DeadLetterQuery(String group, long cursor, int limit) {}
    record ExpansionRequest(String topic, int targetPartitions) {}

    private WireValues() {}

    static String identifier(DataInputStream in, String label) throws IOException {
        return Settings.requireIdentifier(Binary.readText(in, Settings.MAX_IDENTIFIER), label);
    }

    static void writePublication(DataOutputStream out, Publication value) throws IOException {
        Binary.writeText(out, value.producer());
        out.writeLong(value.sequence());
        Binary.writeText(out, value.topic());
        out.writeInt(value.partition());
        Binary.writeBytes(out, value.key());
        Binary.writeBytes(out, value.value());
    }

    static Publication readPublication(DataInputStream in, int payloadLimit) throws IOException {
        return new Publication(identifier(in, "producer"), in.readLong(), identifier(in, "topic"),
                in.readInt(), Binary.readBytes(in, Settings.MAX_KEY_BYTES), Binary.readBytes(in, payloadLimit));
    }

    static void writeReceipt(DataOutputStream out, ProduceReceipt receipt) throws IOException {
        Binary.writeText(out, receipt.producer());
        out.writeLong(receipt.sequence());
        MessageV1.writePartition(out, receipt.partition());
        out.writeLong(receipt.offset());
        out.writeLong(receipt.appendedAtMs());
        out.writeBoolean(receipt.replayed());
    }

    static ProduceReceipt readReceipt(DataInputStream in) throws IOException {
        return new ProduceReceipt(identifier(in, "producer"), in.readLong(), MessageV1.readPartition(in),
                in.readLong(), in.readLong(), readFlag(in));
    }

    static void writeNames(DataOutputStream out, Set<String> names) throws IOException {
        out.writeInt(names.size());
        for (String name : new TreeSet<>(names)) {
            Binary.writeText(out, name);
        }
    }

    static Set<String> readNames(DataInputStream in) throws IOException {
        int size = Binary.readCount(in, MAX_TOPICS);
        var names = new TreeSet<String>();
        for (int i = 0; i < size; i++) {
            if (!names.add(identifier(in, "topic"))) {
                throw new IOException("duplicate topic");
            }
        }
        return Collections.unmodifiableSet(names);
    }

    static void writeEpoch(DataOutputStream out, GroupEpoch epoch) throws IOException {
        Binary.writeText(out, epoch.group());
        Binary.writeText(out, epoch.member());
        out.writeLong(epoch.number());
        writePartitions(out, epoch.assignment());
    }

    static GroupEpoch readEpoch(DataInputStream in) throws IOException {
        return new GroupEpoch(identifier(in, "group"), identifier(in, "member"), in.readLong(), readPartitions(in));
    }

    private static void writePartitions(DataOutputStream out, List<TopicPartition> partitions) throws IOException {
        out.writeInt(partitions.size());
        for (TopicPartition partition : partitions) {
            MessageV1.writePartition(out, partition);
        }
    }

    private static List<TopicPartition> readPartitions(DataInputStream in) throws IOException {
        int size = Binary.readCount(in, MAX_PARTITIONS);
        var partitions = new ArrayList<TopicPartition>(size);
        var distinct = new TreeSet<TopicPartition>();
        for (int i = 0; i < size; i++) {
            TopicPartition partition = MessageV1.readPartition(in);
            if (!distinct.add(partition)) {
                throw new IOException("duplicate assigned partition");
            }
            partitions.add(partition);
        }
        return List.copyOf(partitions);
    }

    static void writeLeases(DataOutputStream out, List<DeliveryLease> leases) throws IOException {
        out.writeInt(leases.size());
        for (DeliveryLease lease : leases) {
            out.writeLong(lease.id());
            out.writeLong(lease.epoch());
            out.writeLong(lease.reserveUntilMs());
            out.writeInt(lease.attempt());
            MessageV1.writePartition(out, lease.message().partition());
            MessageV1.write(out, lease.message());
        }
    }

    static List<DeliveryLease> readLeases(DataInputStream in) throws IOException {
        int count = Binary.readCount(in, MAX_BATCH);
        var leases = new ArrayList<DeliveryLease>(count);
        for (int i = 0; i < count; i++) {
            long id = in.readLong();
            long epoch = in.readLong();
            long reserveUntil = in.readLong();
            int attempt = in.readInt();
            TopicPartition partition = MessageV1.readPartition(in);
            leases.add(new DeliveryLease(id, epoch, reserveUntil, attempt, MessageV1.read(in, partition)));
        }
        return List.copyOf(leases);
    }

    private static void writeRef(DataOutputStream out, DeliveryLease.Ref ref) throws IOException {
        out.writeLong(ref.id());
        MessageV1.writePartition(out, ref.partition());
        out.writeLong(ref.offset());
    }

    private static DeliveryLease.Ref readRef(DataInputStream in) throws IOException {
        long id = in.readLong();
        TopicPartition partition = MessageV1.readPartition(in);
        long offset = in.readLong();
        if (id <= 0 || offset < 0) {
            throw new IOException("invalid lease reference");
        }
        return new DeliveryLease.Ref(id, partition, offset);
    }

    static void writeSettlement(DataOutputStream out, Settlement request) throws IOException {
        out.writeLong(request.epoch());
        out.writeInt(request.refs().size());
        for (DeliveryLease.Ref ref : request.refs()) {
            writeRef(out, ref);
        }
    }

    static Settlement readSettlement(DataInputStream in) throws IOException {
        long epoch = in.readLong();
        int count = Binary.readCount(in, MAX_BATCH);
        var refs = new ArrayList<DeliveryLease.Ref>(count);
        for (int i = 0; i < count; i++) {
            refs.add(readRef(in));
        }
        return new Settlement(epoch, List.copyOf(refs));
    }

    static int acceptanceSize(List<DeliveryLease.Ref> refs) {
        int size = Integer.BYTES;
        for (DeliveryLease.Ref ref : refs) {
            size += 32 + ref.partition().topic().length();
        }
        return size;
    }

    static void writeAcceptances(DataOutputStream out, List<DeliveryLease.Acceptance> receipts) throws IOException {
        out.writeInt(receipts.size());
        for (DeliveryLease.Acceptance receipt : receipts) {
            writeRef(out, receipt.ref());
            out.writeLong(receipt.expiresAtMs());
        }
    }

    static List<DeliveryLease.Acceptance> readAcceptances(DataInputStream in) throws IOException {
        int count = Binary.readCount(in, MAX_BATCH);
        var result = new ArrayList<DeliveryLease.Acceptance>(count);
        for (int i = 0; i < count; i++) {
            result.add(new DeliveryLease.Acceptance(readRef(in), in.readLong()));
        }
        return List.copyOf(result);
    }

    static void writeFrontiers(DataOutputStream out, Map<TopicPartition, Long> frontiers) throws IOException {
        out.writeInt(frontiers.size());
        for (var frontier : frontiers.entrySet()) {
            MessageV1.writePartition(out, frontier.getKey());
            out.writeLong(frontier.getValue());
        }
    }

    static Map<TopicPartition, Long> readFrontiers(DataInputStream in) throws IOException {
        int size = Binary.readCount(in, MAX_PARTITIONS);
        var result = new TreeMap<TopicPartition, Long>();
        for (int i = 0; i < size; i++) {
            putUnique(result, MessageV1.readPartition(in), in.readLong());
        }
        return Collections.unmodifiableMap(result);
    }

    static ExpansionRequest readExpansion(DataInputStream in) throws IOException {
        String topic = identifier(in, "topic");
        int target = in.readInt();
        if (target < 1 || target > MAX_PARTITIONS) {
            throw new IOException("invalid target partition count");
        }
        return new ExpansionRequest(topic, target);
    }

    static void writeExpansion(DataOutputStream out, SegmentStore.Expansion expansion) throws IOException {
        Binary.writeText(out, expansion.topic());
        out.writeInt(expansion.previousPartitions());
        out.writeInt(expansion.partitionCount());
        out.writeInt(expansion.boundaries().size());
        for (long boundary : expansion.boundaries()) {
            out.writeLong(boundary);
        }
    }

    static SegmentStore.Expansion readExpansionResponse(DataInputStream in) throws IOException {
        String topic = identifier(in, "topic");
        int previous = in.readInt();
        int current = in.readInt();
        int boundaries = Binary.readCount(in, MAX_PARTITIONS);
        var values = new ArrayList<Long>(boundaries);
        for (int i = 0; i < boundaries; i++) {
            long value = in.readLong();
            if (value < 0) {
                throw new IOException("negative routing boundary");
            }
            values.add(value);
        }
        var generation = new tidemq.segment.RouteGeneration(current, List.copyOf(values));
        return new SegmentStore.Expansion(topic, previous, current, generation, List.copyOf(values));
    }

    static void writeTopics(DataOutputStream out, Map<String, Integer> topics) throws IOException {
        out.writeInt(topics.size());
        for (var topic : topics.entrySet()) {
            Binary.writeText(out, topic.getKey());
            out.writeInt(topic.getValue());
        }
    }

    static Map<String, Integer> readTopics(DataInputStream in) throws IOException {
        int size = Binary.readCount(in, MAX_TOPICS);
        var topics = new TreeMap<String, Integer>();
        for (int i = 0; i < size; i++) {
            String name = identifier(in, "topic");
            int count = in.readInt();
            if (count < 1 || count > MAX_PARTITIONS) {
                throw new IOException("invalid partition count");
            }
            putUnique(topics, name, count);
        }
        return Collections.unmodifiableMap(topics);
    }

    static void writePartition(DataOutputStream out, SegmentStore.PartitionView view) throws IOException {
        MessageV1.writePartition(out, view.partition());
        out.writeLong(view.range().retainedFrom());
        out.writeLong(view.range().nextOffset());
        out.writeInt(view.segmentCount());
        out.writeInt(view.recent().size());
        for (Segment.View segment : view.recent()) {
            out.writeLong(segment.baseOffset());
            out.writeLong(segment.endOffset());
            out.writeLong(segment.sizeBytes());
            out.writeLong(segment.createdAtMs());
            out.writeLong(segment.latestAppendMs());
            out.writeBoolean(segment.active());
        }
    }

    static SegmentStore.PartitionView readPartition(DataInputStream in) throws IOException {
        TopicPartition partition = MessageV1.readPartition(in);
        var range = new SegmentStore.Range(in.readLong(), in.readLong());
        int total = in.readInt();
        int count = Binary.readCount(in, MAX_SEGMENTS);
        var segments = new ArrayList<Segment.View>(count);
        for (int i = 0; i < count; i++) {
            segments.add(new Segment.View(in.readLong(), in.readLong(), in.readLong(), in.readLong(), in.readLong(),
                    readFlag(in)));
        }
        return new SegmentStore.PartitionView(partition, range, total, List.copyOf(segments));
    }

    static void writeGroup(DataOutputStream out, GroupDirectory.View view) throws IOException {
        Binary.writeText(out, view.name());
        out.writeLong(view.epoch());
        writeNames(out, view.subscriptions());
        out.writeInt(view.assignments().size());
        for (var assignment : view.assignments().entrySet()) {
            Binary.writeText(out, assignment.getKey());
            writePartitions(out, assignment.getValue());
        }
        out.writeInt(view.frontiers().size());
        for (var frontier : view.frontiers().entrySet()) {
            MessageV1.writePartition(out, frontier.getKey());
            out.writeLong(frontier.getValue().next());
            out.writeInt(frontier.getValue().ahead());
            out.writeInt(frontier.getValue().retrying());
        }
    }

    static GroupDirectory.View readGroup(DataInputStream in) throws IOException {
        String name = identifier(in, "group");
        long epoch = in.readLong();
        Set<String> subscriptions = readNames(in);
        int members = Binary.readCount(in, MAX_MEMBERS);
        var assignments = new TreeMap<String, List<TopicPartition>>();
        for (int i = 0; i < members; i++) {
            putUnique(assignments, identifier(in, "member"), readPartitions(in));
        }
        int partitionCount = Binary.readCount(in, MAX_PARTITIONS);
        var frontiers = new TreeMap<TopicPartition, AckFrontier.View>();
        for (int i = 0; i < partitionCount; i++) {
            TopicPartition partition = MessageV1.readPartition(in);
            putUnique(frontiers, partition, new AckFrontier.View(in.readLong(), in.readInt(), in.readInt()));
        }
        return new GroupDirectory.View(name, epoch, subscriptions,
                Collections.unmodifiableMap(assignments), Collections.unmodifiableMap(frontiers));
    }

    static void writeDeadLetters(DataOutputStream out, DeadLetters.Page page) throws IOException {
        out.writeLong(page.nextSequence());
        out.writeInt(page.receipts().size());
        for (DeadLetterReceipt receipt : page.receipts()) {
            DeadLetters.write(out, receipt);
        }
    }

    static DeadLetters.Page readDeadLetters(DataInputStream in) throws IOException {
        long cursor = in.readLong();
        int size = Binary.readCount(in, MAX_BATCH);
        var receipts = new ArrayList<DeadLetterReceipt>(size);
        for (int i = 0; i < size; i++) {
            receipts.add(DeadLetters.read(in));
        }
        return new DeadLetters.Page(cursor, List.copyOf(receipts));
    }

    static void writeRetention(DataOutputStream out, RetentionPreview view) throws IOException {
        MessageV1.writePartition(out, view.partition());
        out.writeLong(view.evaluatedAtMs());
        out.writeLong(view.minimumAgeMs());
        out.writeLong(view.allowedEndExclusive());
        out.writeInt(view.limitingGroupCount());
        out.writeInt(view.limitingGroups().size());
        for (String group : view.limitingGroups()) {
            Binary.writeText(out, group);
        }
        out.writeInt(view.eligibleSegments());
        out.writeLong(view.eligibleBytes());
        out.writeLong(view.resultingLow());
        out.writeLong(view.firstKeptBase());
        out.writeLong(view.firstKeptEnd());
        out.writeBoolean(view.keepsActive());
        out.writeBoolean(view.aboveGroupFrontier());
        out.writeLong(view.remainingAgeMs());
    }

    static RetentionPreview readRetention(DataInputStream in) throws IOException {
        TopicPartition partition = MessageV1.readPartition(in);
        long evaluated = in.readLong();
        long minimumAge = in.readLong();
        long frontier = in.readLong();
        int total = Binary.readCount(in, GroupDirectory.MAX_GROUPS);
        int sampleSize = Binary.readCount(in, Math.min(8, total));
        var names = new ArrayList<String>(sampleSize);
        for (int i = 0; i < sampleSize; i++) {
            names.add(identifier(in, "group"));
        }
        return new RetentionPreview(partition, evaluated, minimumAge, frontier, total, names,
                in.readInt(), in.readLong(), in.readLong(), in.readLong(), in.readLong(), readFlag(in),
                readFlag(in), in.readLong());
    }

    static void writeFlow(DataOutputStream out, FlowView flow) throws IOException {
        Binary.writeText(out, flow.group());
        out.writeLong(flow.epoch());
        out.writeLong(flow.observedAtMs());
        out.writeInt(flow.deliveryWindow());
        out.writeInt(flow.maxInflightPerMember());
        out.writeInt(flow.members());
        out.writeInt(flow.reserved());
        out.writeInt(flow.accepted());
        out.writeInt(flow.sample().size());
        for (var member : flow.sample()) {
            Binary.writeText(out, member.name());
            out.writeInt(member.partitions());
            out.writeInt(member.reserved());
            out.writeInt(member.accepted());
            out.writeLong(member.earliestDeadlineMs());
        }
    }

    static FlowView readFlow(DataInputStream in) throws IOException {
        String group = identifier(in, "group");
        long epoch = in.readLong();
        long observed = in.readLong();
        int window = in.readInt();
        int cap = in.readInt();
        int members = Binary.readCount(in, MAX_MEMBERS);
        int reserved = in.readInt();
        int accepted = in.readInt();
        int shown = Binary.readCount(in, members);
        var sample = new ArrayList<FlowView.Member>(shown);
        for (int i = 0; i < shown; i++) {
            sample.add(new FlowView.Member(identifier(in, "member"), in.readInt(), in.readInt(),
                    in.readInt(), in.readLong()));
        }
        return new FlowView(group, epoch, observed, window, cap, members, reserved, accepted, sample);
    }

    static byte[] error(BrokerException error) throws IOException {
        String message = error.getMessage();
        if (message.length() > MAX_ERROR_CHARS) {
            message = message.substring(0, MAX_ERROR_CHARS);
        }
        String text = message;
        return Binary.encode(out -> {
            out.writeInt(error.code().wire());
            Binary.writeText(out, text);
        });
    }

    static BrokerException readError(DataInputStream in) throws IOException {
        int code = in.readInt();
        String message = Binary.readText(in, MAX_ERROR_CHARS * 4);
        for (BrokerException.Code candidate : BrokerException.Code.values()) {
            if (candidate.wire() == code) {
                return new BrokerException(candidate, message);
            }
        }
        throw new IOException("unknown wire error code " + code);
    }

    private static boolean readFlag(DataInputStream in) throws IOException {
        int value = in.readUnsignedByte();
        if (value != 0 && value != 1) {
            throw new IOException("invalid flag");
        }
        return value == 1;
    }

    private static <K, V> void putUnique(Map<K, V> map, K key, V value) throws IOException {
        if (map.putIfAbsent(key, value) != null) {
            throw new IOException("duplicate map key");
        }
    }
}
