package software.amazon.rds.dbinstance;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import software.amazon.cloudformation.proxy.StdCallbackContext;
import software.amazon.rds.common.handler.TaggingContext;
import software.amazon.rds.common.handler.TimestampContext;

@lombok.Getter
@lombok.Setter
@lombok.ToString
@lombok.EqualsAndHashCode(callSuper = true)
public class CallbackContext extends StdCallbackContext implements TaggingContext.Provider, TimestampContext.Provider {
    private boolean created;
    private boolean deleted;
    private boolean updatedRoles;
    private boolean updated;
    private boolean rebooted;
    private boolean storageAllocated;
    private boolean allocatingStorage;
    private boolean readReplicaPromoted;
    private boolean automaticBackupReplicationStopped;
    private boolean automaticBackupReplicationStarted;
    private String dbInstanceArn;
    private String currentRegion;

    private TaggingContext taggingContext;
    private Map<String, Long> timestamps;
    private Map<String, Double> timeDelta;

    public CallbackContext() {
        super();
        this.taggingContext = new TaggingContext();
        this.timestamps = new HashMap<>();
        this.timeDelta = new HashMap<>();
    }

    @Override
    public TaggingContext getTaggingContext() {
        return taggingContext;
    }

    public boolean isAddTagsComplete() {
        return taggingContext.isAddTagsComplete();
    }

    public void setAddTagsComplete(final boolean addTagsComplete) {
        taggingContext.setAddTagsComplete(addTagsComplete);
    }

    public void timestamp(final String label, final Instant instant) {
        timestamps.put(label, instant.getEpochSecond());
    }

    public void timestampOnce(final String label, final Instant instant) {
        timestamps.computeIfAbsent(label, s -> instant.getEpochSecond());
    }

    public Instant getTimestamp(final String label) {
        if (timestamps.containsKey(label)) {
            return Instant.ofEpochSecond(timestamps.get(label));
        }
        return null;
    }

    public void calculateTimeDelta(final String label, final Instant currentTime, final Instant startTime){
        double delta = Duration.between(currentTime, startTime).toHours();
        timeDelta.put(label, delta);
    }
}
