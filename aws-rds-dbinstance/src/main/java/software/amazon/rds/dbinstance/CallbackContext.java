package software.amazon.rds.dbinstance;

import java.util.HashMap;
import java.util.Map;

import software.amazon.cloudformation.proxy.StdCallbackContext;
import software.amazon.rds.common.handler.TaggingContext;

@lombok.Getter
@lombok.Setter
@lombok.ToString
@lombok.EqualsAndHashCode(callSuper = true)
public class CallbackContext extends StdCallbackContext implements TaggingContext.Provider {
    private boolean created;
    private boolean deleted;
    private boolean updatedRoles;
    private boolean updated;
    private boolean rebooted;

    private Map<String, Integer> probes;
    private TaggingContext taggingContext;

    public CallbackContext() {
        super();
        this.probes = new HashMap<>();
        this.taggingContext = new TaggingContext();
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
