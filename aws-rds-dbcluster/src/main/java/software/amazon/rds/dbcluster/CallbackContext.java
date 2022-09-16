package software.amazon.rds.dbcluster;

import software.amazon.cloudformation.proxy.StdCallbackContext;
import software.amazon.rds.common.handler.TaggingContext;

import java.util.HashMap;
import java.util.Map;

@lombok.Getter
@lombok.Setter
@lombok.ToString
@lombok.EqualsAndHashCode(callSuper = true)
public class CallbackContext extends StdCallbackContext implements TaggingContext.Provider {
    private boolean modified;
    private boolean isRebooted;
    private boolean deleting;

    private TaggingContext taggingContext;
    private final Map<String, Integer> probes;

    public CallbackContext() {
        super();
        this.taggingContext = new TaggingContext();
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
}
