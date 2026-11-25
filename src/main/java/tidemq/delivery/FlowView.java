package tidemq.delivery;

import java.util.List;

public record FlowView(String group, long epoch, long observedAtMs, int deliveryWindow,
                       int maxInflightPerMember, int members, int reserved, int accepted,
                       List<Member> sample) {
    public FlowView {
        sample = List.copyOf(sample);
    }

    public record Member(String name, int partitions, int reserved, int accepted, long earliestDeadlineMs) {}
}
