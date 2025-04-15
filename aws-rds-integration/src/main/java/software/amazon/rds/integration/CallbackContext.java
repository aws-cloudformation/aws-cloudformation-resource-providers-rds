package software.amazon.rds.integration;

import software.amazon.cloudformation.proxy.StdCallbackContext;
import software.amazon.rds.common.handler.TaggingContext;
import software.amazon.rds.common.util.IdempotencyHelper;

@lombok.Getter
@lombok.Setter
@lombok.ToString
@lombok.EqualsAndHashCode(callSuper = true)
public class CallbackContext extends StdCallbackContext implements TaggingContext.Provider, IdempotencyHelper.PreExistenceContext {
    private Boolean preExistenceCheckDone;
    private String integrationArn;

    private TaggingContext taggingContext;
    // used to keep track of post-delete delay in seconds
    // used in software.amazon.rds.integration.DeleteHandler.delay
    private int deleteWaitTime;

    public CallbackContext() {
        super();
        this.taggingContext = new TaggingContext();
    }

    @Override
    public TaggingContext getTaggingContext() {
        return taggingContext;
    }

}
