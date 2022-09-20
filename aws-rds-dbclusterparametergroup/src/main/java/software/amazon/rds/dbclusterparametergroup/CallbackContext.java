package software.amazon.rds.dbclusterparametergroup;

import software.amazon.cloudformation.proxy.StdCallbackContext;
import software.amazon.rds.common.handler.ProbingContext;
import software.amazon.rds.common.handler.TaggingContext;


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

    public CallbackContext() {
        super();
        this.taggingContext = new TaggingContext();
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
}
