package software.amazon.rds.dbshardgroup;

import software.amazon.cloudformation.proxy.StdCallbackContext;
import software.amazon.rds.common.handler.TaggingContext;
import software.amazon.rds.common.util.IdempotencyHelper;
import software.amazon.rds.common.util.WaiterHelper;


@lombok.Getter
@lombok.Setter
@lombok.ToString
@lombok.EqualsAndHashCode(callSuper = true)
public class CallbackContext extends StdCallbackContext implements TaggingContext.Provider, IdempotencyHelper.PreExistenceContext, WaiterHelper.DelayContext {
    private Boolean preExistenceCheckDone;
    private boolean described;
    private boolean updated;

    private TaggingContext taggingContext;

    public CallbackContext() {
        super();
        this.taggingContext = new TaggingContext();
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

    private String dbClusterIdentifier;

    private int waitTime;
}
