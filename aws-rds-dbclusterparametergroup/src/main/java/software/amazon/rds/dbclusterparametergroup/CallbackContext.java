package software.amazon.rds.dbclusterparametergroup;

import software.amazon.cloudformation.proxy.StdCallbackContext;
import software.amazon.rds.common.handler.ProbingContext;
import software.amazon.rds.common.handler.TaggingContext;
import software.amazon.rds.common.handler.TimestampContext;
import software.amazon.rds.common.util.IdempotencyHelper;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;


@lombok.Getter
@lombok.Setter
@lombok.ToString
@lombok.EqualsAndHashCode(callSuper = true)
public class CallbackContext extends StdCallbackContext implements TaggingContext.Provider, ProbingContext.Provider, TimestampContext.Provider, IdempotencyHelper.PreExistenceContext {
    private String marker;
    private String dbClusterParameterGroupArn;

    private Boolean preExistenceCheckDone;
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

    @Override
    public void timestamp(final String label, final Instant instant) {
        timestamps.put(label, instant.getEpochSecond());
    }

    @Override
    public void timestampOnce(final String label, final Instant instant) {
        timestamps.computeIfAbsent(label, s -> instant.getEpochSecond());
    }

    @Override
    public Instant getTimestamp(final String label) {
        if (timestamps.containsKey(label)) {
            return Instant.ofEpochSecond(timestamps.get(label));
        }
        return null;
    }

    @Override
    public void calculateTimeDeltaInMinutes(final String label, final Instant currentTime, final Instant startTime){
        double delta = Duration.between(startTime, currentTime).toMinutes();
        timeDelta.put(label, delta);
    }
}
