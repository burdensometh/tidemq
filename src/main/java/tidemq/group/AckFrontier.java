package tidemq.group;

import java.util.Collections;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;

public final class AckFrontier {
    public record View(long next, int ahead, int retrying) {}

    private long next;
    private final NavigableSet<Long> completed;
    private final NavigableMap<Long, Integer> attempts;

    AckFrontier(long next) {
        this(next, new TreeSet<>(), new TreeMap<>());
    }

    AckFrontier(long next, NavigableSet<Long> completed, NavigableMap<Long, Integer> attempts) {
        this.next = next;
        this.completed = completed;
        this.attempts = attempts;
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

    public int beginAttempt(long offset) {
        if (isComplete(offset)) {
            throw new IllegalStateException("attempt to deliver a completed offset " + offset);
        }
        int count = attempts(offset) + 1;
        attempts.put(offset, count);
        return count;
    }

    public void complete(long offset) {
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

    public java.util.List<Long> exhausted(int limit) {
        return attempts.entrySet().stream().filter(entry -> entry.getValue() >= limit)
                .map(java.util.Map.Entry::getKey).toList();
    }

    View view() {
        return new View(next, completed.size(), attempts.size());
    }
}
