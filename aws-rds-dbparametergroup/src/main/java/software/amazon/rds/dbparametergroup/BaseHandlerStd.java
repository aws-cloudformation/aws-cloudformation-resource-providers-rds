package software.amazon.rds.dbparametergroup;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.amazonaws.util.StringUtils;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DescribeDbParametersResponse;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.rds.model.Parameter;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.delay.Constant;

public abstract class BaseHandlerStd extends BaseHandler<CallbackContext> {
    protected static int MAX_LENGTH_GROUP_NAME = 255;
    protected static int NO_CALLBACK_DELAY = 0;
    protected static int CALLBACK_DELAY_SECONDS = 5 * 60;
    protected static int MAX_DEPTH = 70; //max depth to avoid infinite loop. Maximum parameters in engine ≈ 700 with factor 10
    protected static int RECORDS_PER_PAGE = 100;
    protected static int MAX_PARAMETERS_PER_REQUEST = 20;
    protected static final Constant CONSTANT = Constant.of().timeout(Duration.ofMinutes(120L))
            .delay(Duration.ofSeconds(30L)).build();

    @Override
    public final ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final Logger logger) {
        return handleRequest(
                proxy,
                request,
                callbackContext != null ? callbackContext : new CallbackContext(),
                proxy.newProxy(ClientBuilder::getClient),
                logger
        );
    }

    protected ProgressEvent<ResourceModel, CallbackContext> applyParameters(final AmazonWebServicesClientProxy proxy,
                                                                            final ProxyClient<RdsClient> proxyClient,
                                                                            final ResourceModel model,
                                                                            final CallbackContext callbackContext) {
        //isParametersApplied flag for unit testing
        if (callbackContext.isParametersApplied())
            return ProgressEvent.defaultInProgressHandler(callbackContext, NO_CALLBACK_DELAY, model);

        callbackContext.setParametersApplied(true);
        ProgressEvent<ResourceModel, CallbackContext> progress = ProgressEvent.defaultInProgressHandler(callbackContext, CALLBACK_DELAY_SECONDS, model);

        if (model.getParameters().isEmpty()) return progress; //no parameters to be modified

        // check if provided parameter is supported by rds default engine parameters
        Set<String> paramNames = new HashSet<>(model.getParameters().keySet());
        final Set<Parameter> params = getTargetDefaultParameters(proxyClient, model);

        // subtract set of found and modified params
        paramNames.removeAll(params.stream().map(Parameter::parameterName).collect(Collectors.toSet()));
        if (!paramNames.isEmpty())
            throw new CfnInvalidRequestException("Invalid / Unsupported DB Parameter: " + paramNames.stream().findFirst().get());

        for (List<Parameter> paramsPartition : Iterables.partition(params, MAX_PARAMETERS_PER_REQUEST)) {  //modify api call is limited to 20 parameter per request
            progress = proxy.initiate("rds::modify-db-parameter-group", proxyClient, model, callbackContext)
                    .translateToServiceRequest((resourceModel) -> Translator.modifyDbParameterGroupRequest(resourceModel, paramsPartition))
                    .makeServiceCall((request, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(request, proxyInvocation.client()::modifyDBParameterGroup))
                    .progress(CALLBACK_DELAY_SECONDS);

            if (!progress.isSuccess()) return progress;
        }

        return ProgressEvent.progress(progress.getResourceModel(), progress.getCallbackContext());
    }

    private Set<Parameter> getTargetDefaultParameters(final ProxyClient<RdsClient> proxyClient, final ResourceModel model) {
        String marker = null;
        int depth = 0;
        final Set<Parameter> params = new HashSet<>();
        //iterating on all default parameters to choose the one will be modified
        do {
            final DescribeDbParametersResponse dbParametersResponse = proxyClient.injectCredentialsAndInvokeV2(
                    Translator.describeDbParameterGroupsRequest(model, marker, RECORDS_PER_PAGE), proxyClient.client()::describeDBParameters);
            marker = dbParametersResponse.marker();
            //Translate and checking if it is modifiable
            params.addAll(Translator.getParametersToModify(model, dbParametersResponse.parameters()));
        } while (!StringUtils.isNullOrEmpty(marker) && depth <= MAX_DEPTH);
        return params;
    }

    protected ProgressEvent<ResourceModel, CallbackContext> tagResource(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Map<String, String> tags
    ) {
        return proxy.initiate("rds::tag-db-parameter-group", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(Translator::describeDbParameterGroupsRequest)
                .makeServiceCall(((describeDbGroupsRequest, proxyInvocation) -> {
                    return proxyInvocation.injectCredentialsAndInvokeV2(describeDbGroupsRequest, proxyInvocation.client()::describeDBParameterGroups);
                }))
                .done((request, response, invocation, model, context) -> {
                    final String arn = response.dbParameterGroups().stream().findFirst().get().dbParameterGroupArn();
                    final Set<Tag> currentTags = Translator.translateTagsToModelResource(tags);
                    final Set<Tag> existingTags = listTags(proxyClient, arn);
                    final Set<Tag> tagsToRemove = Sets.difference(existingTags, currentTags);
                    final Set<Tag> tagsToAdd = Sets.difference(currentTags, existingTags);

                    proxyClient.injectCredentialsAndInvokeV2(
                            Translator.removeTagsFromResourceRequest(arn, tagsToRemove),
                            proxyClient.client()::removeTagsFromResource
                    );
                    proxyClient.injectCredentialsAndInvokeV2(
                            Translator.addTagsToResourceRequest(arn, tagsToAdd),
                            proxyClient.client()::addTagsToResource
                    );
                    return ProgressEvent.progress(model, context);
                });
    }

    protected Set<Tag> listTags(final ProxyClient<RdsClient> proxyClient,
                                final String arn) {
        final ListTagsForResourceResponse listTagsForResourceResponse = proxyClient.injectCredentialsAndInvokeV2(
                Translator.listTagsForResourceRequest(arn),
                proxyClient.client()::listTagsForResource);
        return Translator.translateTagsFromSdk(listTagsForResourceResponse.tagList());
    }

    protected abstract ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger);
}
