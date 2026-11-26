package tidemq.group;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;

public final class AckFrontier {
    public record View(long next, int ahead, int retrying) {}

    private long next;
    private final NavigableSet<Long> completed;
    private final NavigableMap<Long, Integer> attempts;
    private final NavigableMap<Long, Long> retryDeadlines;

    AckFrontier(long next) {
        this(next, new TreeSet<>(), new TreeMap<>(), new TreeMap<>());
    }

    AckFrontier(long next, NavigableSet<Long> completed, NavigableMap<Long, Integer> attempts) {
        this(next, completed, attempts, new TreeMap<>());
    }

    AckFrontier(long next, NavigableSet<Long> completed, NavigableMap<Long, Integer> attempts,
                NavigableMap<Long, Long> retryDeadlines) {
        this.next = next;
        this.completed = completed;
        this.attempts = attempts;
        this.retryDeadlines = retryDeadlines;
    }

    public long next() {
        return next;
    }

    public boolean isComplete(long offset) {
        return offset < next || completed.contains(offset);
    }

    public int attempts(long offset) {
        return attempts.getOrDefault(offset, 0);
    }

    /**
     * True while an offset that already consumed an accepted attempt is serving its retry
     * backoff. Such an offset must not be reserved before its deadline, but must not hold back
     * other offsets behind the same partition frontier.
     */
    public boolean isWaiting(long offset, long now) {
        Long deadline = retryDeadlines.get(offset);
        return deadline != null && now < deadline;
    }

    public long retryDeadline(long offset) {
        return retryDeadlines.getOrDefault(offset, -1L);
    }

    /** Offsets that consumed at least one attempt and are not yet complete. */
    public List<Long> unconsumedAttempted() {
        var result = new ArrayList<Long>();
        for (var attempt : attempts.entrySet()) {
            if (!isComplete(attempt.getKey())) {
                result.add(attempt.getKey());
            }
        }
        return result;
    }

    /** Records when the next attempt for a consumed-but-unsettled offset may become available. */
    public void scheduleRetry(long offset, long atMs) {
        if (isComplete(offset)) {
            retryDeadlines.remove(offset);
            return;
        }
        retryDeadlines.merge(offset, atMs, Math::max);
    }

    public int beginAttempt(long offset) {
        if (isComplete(offset)) {
            throw new IllegalStateException("attempt to deliver a completed offset " + offset);
        }
        retryDeadlines.remove(offset);
        int count = attempts(offset) + 1;
        attempts.put(offset, count);
        return count;
    }

    public void complete(long offset) {
        retryDeadlines.remove(offset);
        if (isComplete(offset)) {
            return;
        }
        attempts.remove(offset);
        if (offset != next) {
            completed.add(offset);
            return;
        }
        next++;
        while (completed.remove(next)) {
            next++;
        }
    }

    NavigableSet<Long> completedAhead() {
        return Collections.unmodifiableNavigableSet(completed);
    }

    NavigableMap<Long, Integer> retrying() {
        return Collections.unmodifiableNavigableMap(attempts);
    }

    NavigableMap<Long, Long> retryDeadlines() {
        return Collections.unmodifiableNavigableMap(retryDeadlines);
    }

    public List<Long> exhausted(int limit) {
        return attempts.entrySet().stream().filter(entry -> entry.getValue() >= limit)
                .map(java.util.Map.Entry::getKey).toList();
    }

    View view() {
        return new View(next, completed.size(), attempts.size());
    }
}
