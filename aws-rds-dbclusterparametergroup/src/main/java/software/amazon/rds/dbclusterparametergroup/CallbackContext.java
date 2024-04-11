package software.amazon.rds.dbclusterparametergroup;

import software.amazon.cloudformation.proxy.StdCallbackContext;
import software.amazon.rds.common.handler.ProbingContext;
import software.amazon.rds.common.handler.TaggingContext;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;


@lombok.Getter
@lombok.Setter
@lombok.ToString
@lombok.EqualsAndHashCode(callSuper = true)
public class CallbackContext extends StdCallbackContext implements TaggingContext.Provider, ProbingContext.Provider {
    private String marker;
    private String dbClusterParameterGroupArn;

    private boolean parametersApplied;
    private boolean clusterStabilized;
    private boolean parametersModified;

    private TaggingContext taggingContext;
    private ProbingContext probingContext;

    private Map<String, Long> timestamps;
    private Map<String, Double> timeDelta;

    public CallbackContext() {
        super();
        this.taggingContext = new TaggingContext();
        this.probingContext = new ProbingContext();
        this.timestamps = new HashMap<>();
        this.timeDelta = new HashMap<>();
    }

    @Override
    public ProbingContext getProbingContext() { return probingContext; }

    @Override
    public TaggingContext getTaggingContext() {
        return taggingContext;
    }

    public boolean isAddTagsComplete() {
        return taggingContext.isAddTagsComplete();
    }

    public void setAddTagsComplete(final boolean addTagsComplete) {
        this.taggingContext.setAddTagsComplete(addTagsComplete);
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
