package software.amazon.rds.dbinstance;

import java.util.HashMap;
import java.util.Map;

import software.amazon.cloudformation.proxy.StdCallbackContext;

@lombok.Getter
@lombok.Setter
@lombok.ToString
@lombok.EqualsAndHashCode(callSuper = true)
public class CallbackContext extends StdCallbackContext {
    private boolean created;
    private boolean deleted;
    private boolean updatedRoles;
    private boolean updated;
    private boolean rebooted;
    private boolean createTagComplete;
    private boolean softFailTags;

    private Map<String, Integer> probes;

    public CallbackContext() {
        super();
        this.probes = new HashMap<>();
    }

    public int getProbes(final String sampleName) {
        return this.probes.getOrDefault(sampleName, 0);
    }

    public int incProbes(final String sampleName) {
        return this.probes.merge(sampleName, 1, Integer::sum);
    }

    public void flushProbes(final String sampleName) {
        this.probes.remove(sampleName);
    }
}
