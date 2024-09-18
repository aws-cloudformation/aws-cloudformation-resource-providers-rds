package software.amazon.rds.dbshardgroup;

import java.util.function.Function;

import com.amazonaws.util.StringUtils;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.common.util.IdentifierFactory;

import java.util.HashSet;

public class CreateHandler extends BaseHandlerStd {

    public static final String STACK_NAME = "rds";
    public static final String RESOURCE_IDENTIFIER = "dbshardgroup";
    public static final int RESOURCE_ID_MAX_LENGTH = 63;

    public static final IdentifierFactory dbShardGroupIdentifierFactory = new IdentifierFactory(
            STACK_NAME,
            RESOURCE_IDENTIFIER,
            RESOURCE_ID_MAX_LENGTH
    );

    /** Default constructor w/ default backoff */
    public CreateHandler() {
        this(DB_SHARD_GROUP_HANDLER_CONFIG_36H);
    }

    /** Default constructor w/ custom config */
    public CreateHandler(HandlerConfig config) {
        super(config);
    }

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext) {
        final Tagging.TagSet allTags = Tagging.TagSet.builder()
                .systemTags(Tagging.translateTagsToSdk(request.getSystemTags()))
                .stackTags(Tagging.translateTagsToSdk(request.getDesiredResourceTags()))
                .resourceTags(new HashSet<>(Translator.translateTagsToSdk(request.getDesiredResourceState().getTags())))
                .build();
        ResourceModel model = request.getDesiredResourceState();
        if (StringUtils.isNullOrEmpty(model.getDBShardGroupIdentifier())) {
            model.setDBShardGroupIdentifier(
                    dbShardGroupIdentifierFactory.newIdentifier()
                            .withStackId(request.getStackId())
                            .withResourceId(request.getLogicalResourceIdentifier())
                            .withRequestToken(request.getClientRequestToken())
                            .toString());
        }

        return ProgressEvent.progress(request.getDesiredResourceState(), callbackContext)
                .then(progress -> createDbShardGroup(proxy, proxyClient, progress))
                .then(progress -> Commons.execOnce(progress, () -> addTags(proxyClient, request, progress, allTags), CallbackContext::isAddTagsComplete, CallbackContext::setAddTagsComplete))
                .then(progress -> new ReadHandler().handleRequest(proxy, proxyClient, request, callbackContext));
    }

    private ProgressEvent<ResourceModel, CallbackContext> createDbShardGroup(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress
    ) {
        return proxy.initiate("rds::create-db-shard-group", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest((resourceModel) -> Translator.createDbShardGroupRequest(resourceModel))
                .backoffDelay(config.getBackoff())
                .makeServiceCall((createDbShardGroupRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(createDbShardGroupRequest, proxyInvocation.client()::createDBShardGroup))
                .stabilize((createRequest, createResponse, proxyInvocation, model, context) -> isDBShardGroupStabilizedAfterMutate(model, proxyInvocation))
                .handleError((deleteRequest, exception, client, resourceModel, ctx) -> Commons.handleException(
                        ProgressEvent.progress(resourceModel, ctx),
                        exception,
                        DEFAULT_DB_SHARD_GROUP_ERROR_RULE_SET,
                        requestLogger
                ))
                .progress();
    }
}
