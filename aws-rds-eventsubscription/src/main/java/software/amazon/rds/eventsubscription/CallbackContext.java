package software.amazon.rds.eventsubscription;

import software.amazon.cloudformation.proxy.StdCallbackContext;
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
public class CallbackContext extends StdCallbackContext implements TaggingContext.Provider, TimestampContext.Provider, IdempotencyHelper.PreExistenceContext {
    private Boolean preExistenceCheckDone;
    private String eventSubscriptionArn;

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
