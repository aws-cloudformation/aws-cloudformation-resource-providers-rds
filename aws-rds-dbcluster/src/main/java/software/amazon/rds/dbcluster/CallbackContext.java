package software.amazon.rds.dbcluster;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import software.amazon.cloudformation.proxy.StdCallbackContext;
import software.amazon.rds.common.handler.ProbingContext;
import software.amazon.rds.common.handler.TaggingContext;

@lombok.Getter
@lombok.Setter
@lombok.ToString
@lombok.EqualsAndHashCode(callSuper = true)
public class CallbackContext extends StdCallbackContext implements TaggingContext.Provider, ProbingContext.Provider {
    private boolean modified;
    private boolean rebooted;
    private boolean deleting;

    private Map<String, Long> timestamps;

    private TaggingContext taggingContext;
    private ProbingContext probingContext;

    public CallbackContext() {
        super();
        this.taggingContext = new TaggingContext();
        this.probingContext = new ProbingContext();
        this.timestamps = new HashMap<>();
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
    public ProbingContext getProbingContext() {
        return probingContext;
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
}
