package software.amazon.rds.dbclusterparametergroup;

import com.amazonaws.util.StringUtils;
import com.google.common.collect.Sets;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.Set;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DbParameterGroupNotFoundException;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParameterGroupsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParameterGroupsResponse;
import software.amazon.awssdk.services.rds.model.Parameter;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParametersResponse;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.CallChain.Completed;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;


import java.util.stream.Collectors;

import static software.amazon.rds.dbclusterparametergroup.Translator.mapToTags;

public abstract class BaseHandlerStd extends BaseHandler<CallbackContext> {

    protected static int MAX_LENGTH_GROUP_NAME = 255;
    protected static int CALLBACK_DELAY_SECONDS = 5 * 60; // 5 min for propagation
    protected static int NO_CALLBACK_DELAY = 0;


    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(final AmazonWebServicesClientProxy proxy,
                                                                       final ResourceHandlerRequest<ResourceModel> request,
                                                                       final CallbackContext callbackContext,
                                                                       final Logger logger) {
        return handleRequest(proxy, request, callbackContext != null ? callbackContext : new CallbackContext(), proxy.newProxy(ClientBuilder::getClient), logger);
    }

    protected abstract ProgressEvent<ResourceModel, CallbackContext> handleRequest(AmazonWebServicesClientProxy proxy,
                                                                                   ResourceHandlerRequest<ResourceModel> request,
                                                                                   CallbackContext callbackContext,
                                                                                   ProxyClient<RdsClient> client,
                                                                                   Logger logger);

    protected ProgressEvent<ResourceModel, CallbackContext> applyParameters(final AmazonWebServicesClientProxy proxy,
                                                                            final ProxyClient<RdsClient> proxyClient,
                                                                            final ResourceModel model,
                                                                            final CallbackContext callbackContext) {
        if (callbackContext.isParametersApplied()) return ProgressEvent.defaultInProgressHandler(callbackContext, NO_CALLBACK_DELAY, model);

        callbackContext.setParametersApplied(true);
        ProgressEvent<ResourceModel, CallbackContext> progress = ProgressEvent.defaultInProgressHandler(callbackContext, CALLBACK_DELAY_SECONDS, model);

        if (model.getParameters().isEmpty()) return progress;

        // check if provided parameter is supported by rds
        Set<String> paramNames = model.getParameters().keySet();

        String marker = null;
        do {

            final DescribeDbClusterParametersResponse dbClusterParametersResponse = proxyClient.injectCredentialsAndInvokeV2(
                    Translator.describeDbClusterParametersRequest(model, marker), proxyClient.client()::describeDBClusterParameters);

            marker = dbClusterParametersResponse.marker();
            final Set<Parameter> params = Translator.getParametersToModify(model, dbClusterParametersResponse.parameters());

            // if no params need to be modified then skip the api invocation
            if (params.isEmpty()) continue;

            // substract set of found and modified params
            paramNames.removeAll(params.stream().map(Parameter::parameterName).collect(Collectors.toSet()));

            progress = proxy.initiate("rds::modify-db-cluster-parameter-group::" + marker, proxyClient, model, callbackContext)
                    .translateToServiceRequest((resourceModel) -> Translator.modifyDbClusterParameterGroupRequest(resourceModel, params))
                    .makeServiceCall((request, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(request, proxyInvocation.client()::modifyDBClusterParameterGroup))
                    .progress(CALLBACK_DELAY_SECONDS);
        } while (!StringUtils.isNullOrEmpty(marker));
        // if there are parameters left that couldn't be found in rds api then they are invalid
        if (!paramNames.isEmpty()) throw new CfnInvalidRequestException("Invalid / Unsupported DB Parameter: " + paramNames.stream().findFirst().get());

        return progress;
    }

    protected Completed<DescribeDbClusterParameterGroupsRequest,
                DescribeDbClusterParameterGroupsResponse,
                RdsClient,
                ResourceModel,
                CallbackContext> describeDbClusterParameterGroup(final AmazonWebServicesClientProxy proxy,
                                                             final ProxyClient<RdsClient> proxyClient,
                                                             final ResourceModel model,
                                                             final CallbackContext callbackContext) {
        return proxy.initiate("rds::describe-db-cluster-parameter-group::", proxyClient, model, callbackContext)
                .translateToServiceRequest(Translator::describeDbClusterParameterGroupsRequest)
                .makeServiceCall((describeDbClusterParameterGroupsRequest, rdsClientProxyClient) -> rdsClientProxyClient.injectCredentialsAndInvokeV2(describeDbClusterParameterGroupsRequest, rdsClientProxyClient.client()::describeDBClusterParameterGroups))
                .handleError((describeDbClusterParameterGroupsRequest, exception, client, resourceModel, cxt) -> {
                    if (exception instanceof DbParameterGroupNotFoundException)
                        return ProgressEvent.defaultFailureHandler(exception, HandlerErrorCode.NotFound);
                    throw exception;
                });
    }



    protected ProgressEvent<ResourceModel, CallbackContext> tagResource(final DescribeDbClusterParameterGroupsResponse describeDbClusterParameterGroupsResponse,
                                                                        final ProxyClient<RdsClient> proxyClient,
                                                                        final ResourceModel model,
                                                                        final CallbackContext callbackContext,
                                                                        final Map<String, String> tags) {
        final String arn = describeDbClusterParameterGroupsResponse.dbClusterParameterGroups().stream().findFirst().get().dbClusterParameterGroupArn();

        final Set<Tag> currentTags = mapToTags(tags);
        final Set<Tag> existingTags = listTags(proxyClient, arn);
        final Set<Tag> tagsToRemove = Sets.difference(existingTags, currentTags);
        final Set<Tag> tagsToAdd = Sets.difference(currentTags, existingTags);

        proxyClient.injectCredentialsAndInvokeV2(Translator.removeTagsFromResourceRequest(arn, tagsToRemove), proxyClient.client()::removeTagsFromResource);
        proxyClient.injectCredentialsAndInvokeV2(Translator.addTagsToResourceRequest(arn, tagsToAdd), proxyClient.client()::addTagsToResource);
        return ProgressEvent.progress(model, callbackContext);
    }

    protected Set<Tag> listTags(final ProxyClient<RdsClient> proxyClient,
                                final String arn) {
        final ListTagsForResourceResponse listTagsForResourceResponse = proxyClient.injectCredentialsAndInvokeV2(Translator.listTagsForResourceRequest(arn), proxyClient.client()::listTagsForResource);
        return Translator.translateTagsFromSdk(listTagsForResourceResponse.tagList());
    }
}
