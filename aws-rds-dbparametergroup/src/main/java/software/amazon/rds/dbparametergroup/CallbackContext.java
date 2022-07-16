package software.amazon.rds.dbparametergroup;

import com.google.common.collect.Maps;
import software.amazon.awssdk.services.rds.model.Parameter;
import software.amazon.cloudformation.proxy.StdCallbackContext;
import software.amazon.rds.common.handler.TaggingContext;

import java.util.Map;

@lombok.Getter
@lombok.Setter
@lombok.ToString
@lombok.EqualsAndHashCode(callSuper = true)
public class CallbackContext extends StdCallbackContext implements TaggingContext.Provider {
    private boolean parameterGroupCreated;

    private String defaultParametersMarker;
    private boolean defaultParametersFetched;
    private Map<String, Parameter> defaultParameters;

    private String currentParametersMarker;
    private boolean currentParametersFetched;
    private Map<String, Parameter> currentParameters;

    private boolean verifiedParameterGroupExists;

    private boolean parametersApplied;
    private String dbParameterGroupArn;

    private TaggingContext taggingContext;

    public CallbackContext() {
        super();
        this.taggingContext = new TaggingContext();
        this.defaultParameters = Maps.newHashMap();
        this.currentParameters = Maps.newHashMap();
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
