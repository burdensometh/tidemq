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
                frontier.complete(message.offset());
                changed.add(group);
            }
        });
        for (GroupState group : groups.states()) {
            for (TopicPartition partition : group.partitions()) {
                AckFrontier frontier = group.frontier(partition);
                for (long offset : frontier.exhausted(settings.maxAttempts())) {
                    terminal(group, partition, offset, now);
                    changed.add(group);
                }
            }
        }
        for (GroupState group : changed) {
            groups.saveProgress(group);
        }
    }

    public void invalidate(String group, long now) throws IOException {
        LeaseBook book = books.remove(group);
        if (book != null) {
            dispose(groups.require(group), book, book.entries(), now);
        }
    }

    /**
     * Applies a membership transition without discarding deliveries that are still
     * owned by a live session. Partitions that moved to another owner enter a
     * cooperative handoff: the source session keeps its leases and only it may
     * settle them, while the destination skips the partition. Sessions that ended
     * lose their leases immediately so receivers can continue unfinished offsets.
     */
    public void reconcile(String groupName, GroupState.Ownership before, long now) throws IOException {
        GroupState group = groups.require(groupName);
        LeaseBook book = books.computeIfAbsent(groupName, name -> new LeaseBook());
        var releasedByEndedSession = new HashSet<TopicPartition>();
        for (var prior : before.sessions().entrySet()) {
            String member = prior.getKey();
            long session = prior.getValue();
            if (group.sessionOf(member) == session) {
                continue;
            }
            for (LeaseBook.Handoff handoff : book.unblockSession(session)) {
                releasedByEndedSession.add(handoff.partition());
            }
            dispose(group, book, book.entriesForSession(session), now);
        }
        GroupState.Ownership after = group.ownership();
        for (TopicPartition partition : group.partitions()) {
            GroupState.Owner oldOwner = before.partitions().get(partition);
            GroupState.Owner currentOwner = after.partitions().get(partition);
            LeaseBook.Handoff handoff = book.handoff(partition);
            if (handoff != null) {
                if (currentOwner != null && handoff.session() == currentOwner.session()
                        && handoff.member().equals(currentOwner.member())) {
                    book.unblockHandoff(partition);
                }
                continue;
            }
            if (oldOwner == null) {
                continue;
            }
            // The nominal owner may shuffle between survivors in the same rebalance that
            // ended a handoff source. That shuffle completes the handoff rather than
            // opening a new one, otherwise survivors could block each other in a cycle.
            if (releasedByEndedSession.contains(partition)) {
                continue;
            }
            if (group.sessionOf(oldOwner.member()) != oldOwner.session()) {
                continue;
            }
            boolean moved = currentOwner == null
                    || !oldOwner.member().equals(currentOwner.member())
                    || oldOwner.session() != currentOwner.session();
            if (moved) {
                book.blockHandoff(partition, oldOwner.member(), oldOwner.session());
            }
        }
    }

    /**
     * Safety net for any session disappearance not observed through
     * {@link #reconcile}: drop blocks and leases whose member session is gone.
     */
    private void releaseDeparted(GroupState group, LeaseBook book, long now) throws IOException {
        List<LeaseBook.Entry> departed = null;
        for (LeaseBook.Entry entry : book.entries()) {
            if (group.sessionOf(entry.member()) != entry.session()) {
                if (departed == null) {
                    departed = new ArrayList<>();
                }
                departed.add(entry);
            }
        }
        if (departed != null) {
            dispose(group, book, departed, now);
        }
        for (LeaseBook.Handoff handoff : List.copyOf(book.handoffs())) {
            if (group.sessionOf(handoff.member()) != handoff.session()) {
                book.unblockHandoff(handoff.partition());
            }
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
        for (LeaseBook.Entry entry : expired) {
            var ref = entry.ref();
            if (entry.accepted() && group.frontier(ref.partition()).attempts(ref.offset()) >= settings.maxAttempts()) {
                terminal(group, ref.partition(), ref.offset(), now);
                advanced = true;
            }
            book.remove(entry);
        }
        if (advanced) {
            groups.saveProgress(group);
        }
    }

    private void terminal(GroupState group, TopicPartition partition, long offset, long now) throws IOException {
        AckFrontier frontier = group.frontier(partition);
        Message message = segments.read(partition, offset);
        deadLetters.archive(group.name(), message, frontier.attempts(offset), now);
        frontier.complete(offset);
    }

    public List<DeliveryLease> pull(GroupEpoch epoch, long session, int limit, int byteBudget, long now)
            throws IOException {
        if (limit < 1 || limit > settings.maxBatch() || byteBudget < 0) {
            throw new BrokerException(BrokerException.Code.BAD_REQUEST, "invalid pull limit");
        }
        GroupState group = groups.require(epoch.group());
        LeaseBook book = books.computeIfAbsent(group.name(), name -> new LeaseBook());
        releaseDeparted(group, book, now);
        expire(group, book, now);
        int capacity = Math.min(limit, settings.maxInflight() - book.count(epoch.member()));
        List<TopicPartition> assigned = epoch.assignment();
        if (capacity == 0 || assigned.isEmpty()) {
            return List.of();
        }
        var cursors = new ArrayList<Cursor>(assigned.size());
        for (TopicPartition partition : assigned) {
            if (isBlocked(group, book, partition)) {
                continue;
            }
            AckFrontier frontier = group.frontier(partition);
            long first = frontier.next();
            long last = Math.min(segments.range(partition).nextOffset(), deadline(first, settings.deliveryWindow()));
            cursors.add(new Cursor(partition, frontier, first, last));
        }
        if (cursors.isEmpty()) {
            return List.of();
        }
        return select(epoch, session, capacity, byteBudget, now, book, cursors);
    }

    /**
     * A transferred partition is invisible to every pull while its source session
     * remains alive. Blocks whose source vanished are cleared defensively.
     */
    private boolean isBlocked(GroupState group, LeaseBook book, TopicPartition partition) {
        LeaseBook.Handoff handoff = book.handoff(partition);
        if (handoff == null) {
            return false;
        }
        if (group.sessionOf(handoff.member()) == handoff.session()) {
            return true;
        }
        book.unblockHandoff(partition);
        return false;
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

    /** Compatibility entry point for callers that already resolved the current epoch. */
    public List<DeliveryLease.Acceptance> accept(GroupEpoch epoch, long session,
                                                 List<DeliveryLease.Ref> refs, long now) throws IOException {
        return accept(epoch.group(), epoch.member(), session, epoch.number(), refs, now);
    }

    public List<DeliveryLease.Acceptance> accept(String groupName, String memberName, long session,
                                                 long requestedEpoch, List<DeliveryLease.Ref> refs, long now)
            throws IOException {
        Settlement settlement = settleGroup(groupName, memberName, session, requestedEpoch);
        GroupState group = settlement.group();
        LeaseBook book = settlement.book();
        releaseDeparted(group, book, now);
        expire(group, book, now);
        List<LeaseBook.Entry> entries = resolveSettlement(settlement, refs);
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

    /** Compatibility entry point for callers that already resolved the current epoch. */
    public Map<TopicPartition, Long> ack(GroupEpoch epoch, long session,
                                         List<DeliveryLease.Ref> refs, long now) throws IOException {
        return ack(epoch.group(), epoch.member(), session, epoch.number(), refs, now);
    }

    public Map<TopicPartition, Long> ack(String groupName, String memberName, long session,
                                         long requestedEpoch, List<DeliveryLease.Ref> refs, long now)
            throws IOException {
        Settlement settlement = settleGroup(groupName, memberName, session, requestedEpoch);
        GroupState group = settlement.group();
        LeaseBook book = settlement.book();
        releaseDeparted(group, book, now);
        expire(group, book, now);
        List<LeaseBook.Entry> entries = resolveSettlement(settlement, refs);
        for (LeaseBook.Entry entry : entries) {
            if (!entry.accepted()) {
                throw new BrokerException(BrokerException.Code.UNKNOWN_LEASE, "accept delivery before acknowledging it");
            }
        }
        var touched = new java.util.TreeSet<TopicPartition>();
        for (LeaseBook.Entry entry : entries) {
            var ref = entry.ref();
            group.frontier(ref.partition()).complete(ref.offset());
            touched.add(ref.partition());
            book.remove(entry);
        }
        groups.saveProgress(group);
        var result = new java.util.TreeMap<TopicPartition, Long>();
        touched.forEach(partition -> result.put(partition, group.frontier(partition).next()));
        return result;
    }

    private Settlement settleGroup(String groupName, String memberName, long session,
                                   long requestedEpoch) throws BrokerException {
        GroupState group = groups.require(groupName);
        GroupEpoch current = groups.current(groupName, memberName, session);
        LeaseBook book = books.computeIfAbsent(groupName, name -> new LeaseBook());
        boolean currentGeneration = requestedEpoch == current.number();
        return new Settlement(group, book, current, session, requestedEpoch, currentGeneration);
    }

    private record Settlement(GroupState group, LeaseBook book, GroupEpoch current, long session,
                              long requestedEpoch, boolean currentGeneration) {}

    private List<LeaseBook.Entry> resolveSettlement(Settlement settlement, List<DeliveryLease.Ref> refs)
            throws BrokerException {
        LeaseBook book = settlement.book();
        GroupState group = settlement.group();
        if (refs.isEmpty() || refs.size() > settings.maxBatch()) {
            throw new BrokerException(BrokerException.Code.BAD_REQUEST, "invalid number of lease references");
        }
        var seen = new HashSet<Long>();
        var entries = new ArrayList<LeaseBook.Entry>(refs.size());
        GroupEpoch current = settlement.current();
        for (DeliveryLease.Ref ref : refs) {
            if (!seen.add(ref.id())) {
                throw new BrokerException(BrokerException.Code.BAD_REQUEST, "duplicate lease reference");
            }
            LeaseBook.Entry entry = book.token(ref.id());
            if (entry == null || entry.session() != settlement.session()
                    || !entry.member().equals(current.member()) || !entry.ref().equals(ref)) {
                throw new BrokerException(BrokerException.Code.UNKNOWN_LEASE,
                        "no active delivery for lease " + ref.id());
            }
            entries.add(entry);
        }
        if (!settlement.currentGeneration()) {
            // A prior generation may settle the session's own batch when it participates
            // in a cooperative handoff. A stale batch touching only partitions the
            // session still owns stays epoch fenced.
            long requested = settlement.requestedEpoch();
            boolean priorGeneration = requested > 0 && requested < current.number();
            boolean handoffBatch = false;
            if (priorGeneration) {
                for (DeliveryLease.Ref ref : refs) {
                    if (transferredAway(group, book, ref.partition(), current.member(), settlement.session())) {
                        handoffBatch = true;
                        break;
                    }
                }
            }
            if (!handoffBatch) {
                throw new BrokerException(BrokerException.Code.STALE_EPOCH,
                        "membership epoch changed to " + current.number());
            }
        }
        return entries;
    }

    /**
     * True when the partition is under cooperative handoff sourced by this session,
     * or its current assignment belongs to another session. Partitions the same
     * session still owns keep strict epoch fencing for stale requests.
     */
    private boolean transferredAway(GroupState group, LeaseBook book, TopicPartition partition,
                                    String member, long session) {
        LeaseBook.Handoff handoff = book.handoff(partition);
        if (handoff != null && handoff.member().equals(member) && handoff.session() == session) {
            return true;
        }
        GroupState.Owner owner = group.ownership().partitions().get(partition);
        return owner != null && (!owner.member().equals(member) || owner.session() != session);
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
