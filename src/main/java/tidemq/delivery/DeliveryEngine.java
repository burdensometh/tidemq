package tidemq.delivery;

import tidemq.BrokerException;
import tidemq.Settings;
import tidemq.deadletter.DeadLetters;
import tidemq.group.AckFrontier;
import tidemq.group.GroupDirectory;
import tidemq.group.GroupEpoch;
import tidemq.group.GroupState;
import tidemq.segment.Message;
import tidemq.segment.SegmentStore;
import tidemq.segment.TopicPartition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public final class DeliveryEngine {
    private final Settings settings;
    private final SegmentStore segments;
    private final GroupDirectory groups;
    private final DeadLetters deadLetters;
    private final Map<String, LeaseBook> books = new HashMap<>();
    private long lastLease;

    public DeliveryEngine(Settings settings, SegmentStore segments, GroupDirectory groups, DeadLetters deadLetters) {
        this.settings = settings;
        this.segments = segments;
        this.groups = groups;
        this.deadLetters = deadLetters;
    }

    public void recover(long now) throws IOException {
        var changed = new HashSet<GroupState>();
        var advanced = new HashMap<GroupState, List<Map.Entry<TopicPartition, Long>>>();
        deadLetters.forEach(receipt -> {
            GroupState group = groups.require(receipt.group());
            Message message = receipt.message();
            AckFrontier frontier = group.frontier(message.partition());
            if (frontier == null || message.offset() >= segments.range(message.partition()).nextOffset()) {
                throw new IOException("dead-letter source outside group partition");
            }
            if (!frontier.isComplete(message.offset())) {
                if (frontier.attempts(message.offset()) != receipt.attempts()) {
                    throw new IOException("dead-letter attempts disagree with group checkpoint");
                }
                long before = frontier.complete(message.offset());
                changed.add(group);
                if (frontier.next() != before) {
                    advanced.computeIfAbsent(group, key -> new ArrayList<>())
                            .add(Map.entry(message.partition(), frontier.next()));
                }
            }
        });
        for (GroupState group : groups.states()) {
            for (TopicPartition partition : group.partitions()) {
                AckFrontier frontier = group.frontier(partition);
                for (long offset : frontier.exhausted(settings.maxAttempts())) {
                    long watermark = terminal(group, partition, offset, now);
                    changed.add(group);
                    if (watermark >= 0) {
                        advanced.computeIfAbsent(group, key -> new ArrayList<>())
                                .add(Map.entry(partition, watermark));
                    }
                }
            }
        }
        for (GroupState group : changed) {
            groups.saveProgress(group);
        }
        for (var entry : advanced.entrySet()) {
            GroupState group = entry.getKey();
            for (Map.Entry<TopicPartition, Long> move : entry.getValue()) {
                groups.watermarkAdvanced(group, move.getKey(), move.getValue());
            }
        }
    }

    public void invalidate(String group, long now) throws IOException {
        LeaseBook book = books.remove(group);
        if (book != null) {
            dispose(groups.require(group), book, book.entries(), now);
        }
    }

    public void sweep(long now) throws IOException {
        for (var entry : books.entrySet()) {
            expire(groups.require(entry.getKey()), entry.getValue(), now);
        }
    }

    public FlowView view(String name, int memberLimit, long now) throws BrokerException {
        var group = groups.view(name);
        var measured = new HashMap<String, Pressure>();
        LeaseBook book = books.get(name);
        if (book != null) {
            book.forEach(entry -> measured.computeIfAbsent(entry.member(), key -> new Pressure()).add(entry));
        }
        int reserved = 0;
        int accepted = 0;
        var sample = new ArrayList<FlowView.Member>();
        for (var entry : group.assignments().entrySet()) {
            String member = entry.getKey();
            Pressure pressure = measured.get(member);
            if (pressure == null) {
                if (sample.size() < memberLimit) {
                    sample.add(new FlowView.Member(member, entry.getValue().size(), 0, 0, -1));
                }
                continue;
            }
            reserved += pressure.reserved;
            accepted += pressure.accepted;
            if (sample.size() < memberLimit) {
                sample.add(new FlowView.Member(member, entry.getValue().size(),
                        pressure.reserved, pressure.accepted, pressure.earliest));
            }
        }
        return new FlowView(name, group.epoch(), now, settings.deliveryWindow(), settings.maxInflight(),
                group.assignments().size(), reserved, accepted, sample);
    }

    private static final class Pressure {
        int reserved;
        int accepted;
        long earliest = Long.MAX_VALUE;

        void add(LeaseBook.Entry entry) {
            if (entry.accepted()) {
                accepted++;
            } else {
                reserved++;
            }
            earliest = Math.min(earliest, entry.activeUntil());
        }
    }

    private void expire(GroupState group, LeaseBook book, long now) throws IOException {
        dispose(group, book, book.expired(now), now);
    }

    private void dispose(GroupState group, LeaseBook book, List<LeaseBook.Entry> expired, long now) throws IOException {
        boolean advanced = false;
        var watermarkMoves = new ArrayList<Map.Entry<TopicPartition, Long>>();
        for (LeaseBook.Entry entry : expired) {
            var ref = entry.ref();
            if (entry.accepted() && group.frontier(ref.partition()).attempts(ref.offset()) >= settings.maxAttempts()) {
                long watermark = terminal(group, ref.partition(), ref.offset(), now);
                advanced = true;
                if (watermark >= 0) {
                    watermarkMoves.add(Map.entry(ref.partition(), watermark));
                }
            }
            book.remove(entry);
        }
        if (advanced) {
            groups.saveProgress(group);
            for (Map.Entry<TopicPartition, Long> move : watermarkMoves) {
                groups.watermarkAdvanced(group, move.getKey(), move.getValue());
            }
        }
    }

    private long terminal(GroupState group, TopicPartition partition, long offset, long now) throws IOException {
        AckFrontier frontier = group.frontier(partition);
        Message message = segments.read(partition, offset);
        deadLetters.archive(group.name(), message, frontier.attempts(offset), now);
        long before = frontier.complete(offset);
        return frontier.next() == before ? -1L : frontier.next();
    }

    public List<DeliveryLease> pull(GroupEpoch epoch, long session, int limit, int byteBudget, long now)
            throws IOException {
        if (limit < 1 || limit > settings.maxBatch() || byteBudget < 0) {
            throw new BrokerException(BrokerException.Code.BAD_REQUEST, "invalid pull limit");
        }
        GroupState group = groups.require(epoch.group());
        LeaseBook book = books.computeIfAbsent(group.name(), name -> new LeaseBook());
        expire(group, book, now);
        int capacity = Math.min(limit, settings.maxInflight() - book.count(epoch.member()));
        List<TopicPartition> assigned = epoch.assignment();
        if (capacity == 0 || assigned.isEmpty()) {
            return List.of();
        }
        var cursors = new ArrayList<Cursor>(assigned.size());
        for (TopicPartition partition : assigned) {
            AckFrontier frontier = group.frontier(partition);
            long first = frontier.next();
            long last = Math.min(segments.range(partition).nextOffset(), deadline(first, settings.deliveryWindow()));
            cursors.add(new Cursor(partition, frontier, first, last));
        }
        return select(epoch, session, capacity, byteBudget, now, book, cursors);
    }

    private List<DeliveryLease> select(GroupEpoch epoch, long session, int limit, int byteBudget, long now,
                                       LeaseBook book, List<Cursor> cursors) throws IOException {
        var leases = new ArrayList<DeliveryLease>(limit);
        int start = book.partitionStart(epoch.member());
        int nextRound = start;
        boolean tooLarge = false;
        boolean added;
        do {
            added = false;
            for (int step = 0; step < cursors.size() && leases.size() < limit; step++) {
                int index = (start + step) % cursors.size();
                Cursor cursor = cursors.get(index);
                Message message = cursor.next(book);
                if (message == null) {
                    continue;
                }
                int attempt = cursor.frontier.attempts(message.offset()) + 1;
                var candidate = new DeliveryLease(0, epoch.number(), 0, attempt, message);
                int size = candidate.wireBytes();
                if (size > byteBudget) {
                    tooLarge = true;
                    cursor.skipRound();
                    continue;
                }
                long id = issueLease();
                leases.add(book.reserve(id, epoch, session, deadline(now, settings.reservationMs()), attempt, message));
                byteBudget -= size;
                added = true;
                nextRound = (index + 1) % cursors.size();
            }
        } while (added && leases.size() < limit);
        book.partitionStart(epoch.member(), nextRound);
        if (leases.isEmpty() && tooLarge) {
            throw new BrokerException(BrokerException.Code.CAPACITY, "available message exceeds negotiated frame");
        }
        return List.copyOf(leases);
    }

    public List<DeliveryLease.Acceptance> accept(GroupEpoch epoch, long session, List<DeliveryLease.Ref> refs, long now)
            throws IOException {
        GroupState group = groups.require(epoch.group());
        LeaseBook book = requireBook(epoch);
        expire(group, book, now);
        List<LeaseBook.Entry> entries = resolve(book, epoch, session, refs);
        boolean changed = false;
        var result = new ArrayList<DeliveryLease.Acceptance>(entries.size());
        for (LeaseBook.Entry entry : entries) {
            var ref = entry.ref();
            if (!entry.accepted()) {
                AckFrontier frontier = group.frontier(ref.partition());
                frontier.beginAttempt(ref.offset());
                entry.accept(deadline(now, settings.leaseMs()));
                changed = true;
            }
            result.add(new DeliveryLease.Acceptance(ref, entry.activeUntil()));
        }
        if (changed) {
            groups.saveProgress(group);
        }
        return List.copyOf(result);
    }

    public Map<TopicPartition, Long> ack(GroupEpoch epoch, long session, List<DeliveryLease.Ref> refs, long now)
            throws IOException {
        GroupState group = groups.require(epoch.group());
        LeaseBook book = requireBook(epoch);
        expire(group, book, now);
        List<LeaseBook.Entry> entries = resolve(book, epoch, session, refs);
        for (LeaseBook.Entry entry : entries) {
            if (!entry.accepted()) {
                throw new BrokerException(BrokerException.Code.UNKNOWN_LEASE, "accept delivery before acknowledging it");
            }
        }
        var touched = new java.util.TreeSet<TopicPartition>();
        var advanced = new java.util.ArrayList<Map.Entry<TopicPartition, Long>>();
        for (LeaseBook.Entry entry : entries) {
            var ref = entry.ref();
            AckFrontier frontier = group.frontier(ref.partition());
            long before = frontier.complete(ref.offset());
            if (frontier.next() != before) {
                advanced.add(Map.entry(ref.partition(), frontier.next()));
            }
            touched.add(ref.partition());
            book.remove(entry);
        }
        groups.saveProgress(group);
        for (Map.Entry<TopicPartition, Long> change : advanced) {
            groups.watermarkAdvanced(group, change.getKey(), change.getValue());
        }
        var result = new java.util.TreeMap<TopicPartition, Long>();
        touched.forEach(partition -> result.put(partition, group.frontier(partition).next()));
        return result;
    }

    private LeaseBook requireBook(GroupEpoch epoch) throws BrokerException {
        LeaseBook book = books.get(epoch.group());
        if (book == null) {
            throw new BrokerException(BrokerException.Code.UNKNOWN_LEASE, "group has no active delivery");
        }
        return book;
    }

    private List<LeaseBook.Entry> resolve(LeaseBook book, GroupEpoch epoch, long session, List<DeliveryLease.Ref> refs)
            throws BrokerException {
        if (refs.isEmpty() || refs.size() > settings.maxBatch()) {
            throw new BrokerException(BrokerException.Code.BAD_REQUEST, "invalid number of lease references");
        }
        var seen = new HashSet<Long>();
        var entries = new ArrayList<LeaseBook.Entry>(refs.size());
        for (DeliveryLease.Ref ref : refs) {
            if (!seen.add(ref.id())) {
                throw new BrokerException(BrokerException.Code.BAD_REQUEST, "duplicate lease reference");
            }
            entries.add(book.require(ref, epoch, session));
        }
        return entries;
    }

    private long issueLease() throws IOException {
        if (lastLease == Long.MAX_VALUE) {
            throw new IOException("lease token space exhausted");
        }
        return ++lastLease;
    }

    private static long deadline(long now, long wait) {
        return now + Math.min(wait, Long.MAX_VALUE - now);
    }

    private final class Cursor {
        final TopicPartition partition;
        final AckFrontier frontier;
        long offset;
        final long end;

        Cursor(TopicPartition partition, AckFrontier frontier, long offset, long end) {
            this.partition = partition;
            this.frontier = frontier;
            this.offset = offset;
            this.end = end;
        }

        Message next(LeaseBook book) throws IOException {
            while (offset < end) {
                long current = offset++;
                if (!frontier.isComplete(current) && !book.has(partition, current)) {
                    return segments.read(partition, current);
                }
            }
            return null;
        }

        void skipRound() {
            offset = end;
        }
    }
}
