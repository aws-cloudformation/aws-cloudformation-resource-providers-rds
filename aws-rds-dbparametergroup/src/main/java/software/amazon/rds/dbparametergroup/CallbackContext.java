package software.amazon.rds.dbparametergroup;

import software.amazon.cloudformation.proxy.StdCallbackContext;
import software.amazon.rds.common.handler.TaggingContext;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@lombok.Getter
@lombok.Setter
@lombok.ToString
@lombok.EqualsAndHashCode(callSuper = true)
public class CallbackContext extends StdCallbackContext implements TaggingContext.Provider {
    private boolean parametersApplied;
    private String dbParameterGroupArn;

    private Map<String, Long> timestamps;
    private Map<String, Double> timeDelta;

    private TaggingContext taggingContext;

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
        if (timestamps.containsKey(label) && label != null) {
            return Instant.ofEpochSecond(timestamps.get(label));
        }
        return null;
    }

    public void calculateTimeDeltaInMinutes(final String label, final Instant currentTime, final Instant startTime){
        double delta = Duration.between(currentTime, startTime).toMinutes();
        timeDelta.put(label, delta);
    }
}
