package tidemq;

import java.io.IOException;

@FunctionalInterface
public interface Boundary {
    Boundary NONE = point -> {};

    enum Point {
        SEGMENT_BEFORE_FORCE,
        SEGMENT_AFTER_FORCE,
        FRAME_BEFORE_REPLY,
        GROUP_BEFORE_REPLACE,
        GROUP_AFTER_REPLACE,
        DEADLETTER_BEFORE_FORCE,
        DEADLETTER_AFTER_FORCE,
        ROUTE_BEFORE_FORCE,
        ROUTE_AFTER_FORCE
    }

    void reached(Point point) throws IOException;
}
