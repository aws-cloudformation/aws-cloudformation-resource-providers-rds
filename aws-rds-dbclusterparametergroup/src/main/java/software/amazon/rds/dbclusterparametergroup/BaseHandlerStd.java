package software.amazon.rds.dbclusterparametergroup;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DbClusterParameterGroupNotFoundException;
import software.amazon.awssdk.services.rds.model.DbParameterGroupAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.DbParameterGroupNotFoundException;
import software.amazon.awssdk.services.rds.model.DbParameterGroupQuotaExceededException;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParameterGroupsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParameterGroupsResponse;
import software.amazon.awssdk.services.rds.model.InvalidDbParameterGroupStateException;
import software.amazon.awssdk.services.rds.model.Parameter;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.CallChain.Completed;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.error.ErrorCode;
import software.amazon.rds.common.error.ErrorRuleSet;
import software.amazon.rds.common.error.ErrorStatus;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.common.logging.LoggingProxyClient;
import software.amazon.rds.common.logging.RequestLogger;
import software.amazon.rds.common.printer.FilteredJsonPrinter;

public abstract class BaseHandlerStd extends BaseHandler<CallbackContext> {
    protected static final BiFunction<ResourceModel, ProxyClient<RdsClient>, ResourceModel> EMPTY_CALL = (model, proxyClient) -> model;
    protected static final String AVAILABLE = "available";
    protected static final String RESOURCE_IDENTIFIER = "dbclusterparametergroup";
    protected static final String STACK_NAME = "rds";
    protected static final int MAX_LENGTH_GROUP_NAME = 255;
    // 5 min for waiting propagation according to https://docs.aws.amazon.com/AmazonRDS/latest/APIReference/API_ModifyDBClusterParameterGroup.html
    protected static final int CALLBACK_DELAY_SECONDS = 5 * 60;
    protected static final int NO_CALLBACK_DELAY = 0;
    protected static final int MAX_PARAMETERS_PER_REQUEST = 20;

    protected static final ErrorRuleSet DEFAULT_DB_CLUSTER_PARAMETER_GROUP_ERROR_RULE_SET = ErrorRuleSet.builder()
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.ResourceConflict),
                    InvalidDbParameterGroupStateException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.AlreadyExists),
                    DbParameterGroupAlreadyExistsException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.ServiceLimitExceeded),
                    DbParameterGroupQuotaExceededException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.NotFound),
                    DbClusterParameterGroupNotFoundException.class,
                    DbParameterGroupNotFoundException.class)
            .build()
            .orElse(Commons.DEFAULT_ERROR_RULE_SET);

    protected static final ErrorRuleSet DB_CLUSTERS_STABILIZATION_ERROR_RULE_SET = ErrorRuleSet.builder()
            .withErrorCodes(ErrorStatus.ignore(),
                    ErrorCode.AccessDeniedException,
                    ErrorCode.AccessDenied,
                    ErrorCode.NotAuthorized)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.NotStabilized),
                    Exception.class)
            .build();

    protected static final ErrorRuleSet SOFT_FAIL_IN_PROGRESS_ERROR_RULE_SET = ErrorRuleSet.builder()
            .withErrorCodes(ErrorStatus.ignore(OperationStatus.IN_PROGRESS),
                    ErrorCode.AccessDenied,
                    ErrorCode.AccessDeniedException)
            .build()
            .orElse(DEFAULT_DB_CLUSTER_PARAMETER_GROUP_ERROR_RULE_SET);

    private final FilteredJsonPrinter PARAMETERS_FILTER = new FilteredJsonPrinter();

    protected HandlerConfig config;

    public BaseHandlerStd(final HandlerConfig config) {
        super();
        this.config = config;
    }

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(final AmazonWebServicesClientProxy proxy,
                                                                       final ResourceHandlerRequest<ResourceModel> request,
                                                                       final CallbackContext callbackContext,
                                                                       final Logger logger) {
        final CallbackContext context = callbackContext != null ? callbackContext : new CallbackContext();
        context.setDbClusterParameterGroupArn(Translator.buildClusterParameterGroupArn(request).toString());
        return RequestLogger.handleRequest(
                logger,
                request,
                PARAMETERS_FILTER,
                requestLogger -> handleRequest(proxy,
                        request,
                        context,
                        new LoggingProxyClient<>(requestLogger, proxy.newProxy(ClientBuilder::getClient)),
                        logger));
    }

    protected abstract ProgressEvent<ResourceModel, CallbackContext> handleRequest(AmazonWebServicesClientProxy proxy,
                                                                                   ResourceHandlerRequest<ResourceModel> request,
                                                                                   CallbackContext callbackContext,
                                                                                   ProxyClient<RdsClient> client,
                                                                                   Logger logger);

    protected ProgressEvent<ResourceModel, CallbackContext> updateTags(final AmazonWebServicesClientProxy proxy,
                                                                       final ProxyClient<RdsClient> rdsProxyClient,
                                                                       final ProgressEvent<ResourceModel, CallbackContext> progress,
                                                                       final Tagging.TagSet previousTags,
                                                                       final Tagging.TagSet desiredTags) {
        final Tagging.TagSet tagsToAdd = Tagging.exclude(desiredTags, previousTags);
        final Tagging.TagSet tagsToRemove = Tagging.exclude(previousTags, desiredTags);

        if (tagsToAdd.isEmpty() && tagsToRemove.isEmpty()) {
            return progress;
        }

        try {
            String arn = progress.getCallbackContext().getDbClusterParameterGroupArn();
            Tagging.removeTags(rdsProxyClient, arn, Tagging.translateTagsToSdk(tagsToRemove));
            Tagging.addTags(rdsProxyClient, arn, Tagging.translateTagsToSdk(tagsToAdd));
        } catch (Exception exception) {
            return Commons.handleException(
                    progress,
                    exception,
                    Tagging.bestEffortErrorRuleSet(tagsToAdd, tagsToRemove, Tagging.SOFT_FAIL_IN_PROGRESS_TAGGING_ERROR_RULE_SET, Tagging.HARD_FAIL_TAG_ERROR_RULE_SET)
                            .orElse(DEFAULT_DB_CLUSTER_PARAMETER_GROUP_ERROR_RULE_SET)
            );
        }

        return progress;
    }

    protected ProgressEvent<ResourceModel, CallbackContext> applyParameters(final AmazonWebServicesClientProxy proxy,
                                                                            final ProxyClient<RdsClient> proxyClient,
                                                                            final ResourceModel model,
                                                                            final CallbackContext callbackContext) {
        //isParametersApplied flag for unit testing
        if (callbackContext.isParametersApplied())
            return ProgressEvent.defaultInProgressHandler(callbackContext, NO_CALLBACK_DELAY, model);
        callbackContext.setParametersApplied(true);

        Map<String, Parameter> currentClusterParameters = Maps.newHashMap();

        return ProgressEvent.progress(model, callbackContext)
                .then(progressEvent -> describeCurrentDBClusterParameters(progressEvent, currentClusterParameters, proxy, proxyClient))
                .then(progressEvent -> validateModelParameters(progressEvent, currentClusterParameters))
                .then(progressEvent -> modifyParameters(progressEvent, currentClusterParameters, proxy, proxyClient))
                .then(progressEvent -> waitForDbClustersStabilization(progressEvent, proxy, proxyClient));
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
                .handleError((describeDbClusterParameterGroupsRequest, exception, client, resourceModel, ctx) -> Commons.handleException(
                        ProgressEvent.progress(resourceModel, ctx),
                        exception,
                        SOFT_FAIL_IN_PROGRESS_ERROR_RULE_SET));
    }


    private ProgressEvent<ResourceModel, CallbackContext> validateModelParameters(
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Map<String, Parameter> defaultEngineParameters
    ) {
        Map<String, Object> modelParameters = Optional.ofNullable(progress.getResourceModel().getParameters()).orElse(Collections.emptyMap());
        Set<String> invalidParameters = modelParameters.entrySet().stream()
                .filter(entry -> {
                    String parameterName = entry.getKey();
                    String newParameterValue = String.valueOf(entry.getValue());
                    Parameter defaultParameter = defaultEngineParameters.get(parameterName);
                    if (!defaultEngineParameters.containsKey(parameterName)) return true;
                    //Parameter is not modifiable and input model contains different value from default value
                    return newParameterValue != null && !defaultParameter.isModifiable()
                            && !newParameterValue.equals(defaultParameter.parameterValue());
                })
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());

        if (!invalidParameters.isEmpty()) {
            return ProgressEvent.failed(
                    progress.getResourceModel(),
                    progress.getCallbackContext(),
                    HandlerErrorCode.InvalidRequest,
                    "Invalid / Unmodifiable / Unsupported DB Parameter: " + invalidParameters.stream().findFirst().get());
        }
        return progress;
    }

    private ProgressEvent<ResourceModel, CallbackContext> describeCurrentDBClusterParameters(final ProgressEvent<ResourceModel, CallbackContext> progress,
                                                                                             final Map<String, Parameter> currentDBClusterParameters,
                                                                                             final AmazonWebServicesClientProxy proxy,
                                                                                             ProxyClient<RdsClient> proxyClient) {
        return proxy.initiate("rds::describe-db-parameters", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(Translator::describeDbClusterParametersRequest)
                .makeServiceCall((request, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeIterableV2(request,
                        proxyInvocation.client()::describeDBClusterParametersPaginator))
                .handleError((describeDBParametersPaginatorRequest, exception, client, resourceModel, ctx) ->
                        Commons.handleException(
                                ProgressEvent.progress(resourceModel, ctx),
                                exception,
                                DEFAULT_DB_CLUSTER_PARAMETER_GROUP_ERROR_RULE_SET))
                .done((describeDbClusterParameterGroupsRequest, describeDbClusterParameterGroupsResponse, proxyInvocation, resourceModel, context) -> {
                    currentDBClusterParameters.putAll(
                            describeDbClusterParameterGroupsResponse.stream()
                                    .flatMap(describeDbClusterParametersResponse -> describeDbClusterParametersResponse.parameters().stream())
                                    .collect(Collectors.toMap(Parameter::parameterName, Function.identity()))
                    );
                    return ProgressEvent.progress(resourceModel, context);
                });
    }

    private Map<String, Parameter> getParametersToModify(final Map<String, Object> modelParameters,
                                                         final Map<String, Parameter> currentDBParameters) {
        Map<String, Parameter> parametersToModify = Maps.newHashMap(currentDBParameters);
        parametersToModify.keySet().retainAll(Optional.ofNullable(modelParameters).orElse(Collections.emptyMap()).keySet());
        return parametersToModify.entrySet()
                .stream()
                //filter to parameters want to modify and its value is different from already exist value
                .filter(entry -> {
                    String parameterName = entry.getKey();
                    String currentParameterValue = entry.getValue().parameterValue();
                    String newParameterValue = String.valueOf(modelParameters.get(parameterName));
                    return !newParameterValue.equals(currentParameterValue);
                })
                .collect(Collectors.toMap(Map.Entry::getKey,
                        entry -> {
                            String parameterName = entry.getKey();
                            String newValue = String.valueOf(modelParameters.get(parameterName));
                            Parameter defaultParameter = entry.getValue();
                            return Translator.buildParameterWithNewValue(newValue, defaultParameter);
                        })
                );
    }

    protected ProgressEvent<ResourceModel, CallbackContext> resetAllParameters(final ProgressEvent<ResourceModel, CallbackContext> progress,
                                                                               final AmazonWebServicesClientProxy proxy,
                                                                               final ProxyClient<RdsClient> proxyClient) {
        return proxy.initiate("rds::reset-db-cluster-parameter-group", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(Translator::resetDbClusterParameterGroupRequest)
                .makeServiceCall((request, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(request, proxyInvocation.client()::resetDBClusterParameterGroup))
                .handleError((resetDbClusterParameterGroupRequest, exception, client, resourceModel, ctx) ->
                        Commons.handleException(
                                ProgressEvent.progress(resourceModel, ctx),
                                exception,
                                DEFAULT_DB_CLUSTER_PARAMETER_GROUP_ERROR_RULE_SET))
                .progress();
    }

    private ProgressEvent<ResourceModel, CallbackContext> modifyParameters(final ProgressEvent<ResourceModel, CallbackContext> progress,
                                                                           final Map<String, Parameter> currentDBParameters,
                                                                           final AmazonWebServicesClientProxy proxy,
                                                                           final ProxyClient<RdsClient> proxyClient) {
        ResourceModel model = progress.getResourceModel();
        CallbackContext callbackContext = progress.getCallbackContext();
        Map<String, Parameter> parametersToModify = getParametersToModify(model.getParameters(), currentDBParameters);
        for (List<Parameter> paramsPartition : Iterables.partition(parametersToModify.values(), MAX_PARAMETERS_PER_REQUEST)) {  //modify api call is limited to 20 parameter per request
            ProgressEvent<ResourceModel, CallbackContext> progressEvent = modifyParameters(proxyClient, proxy, callbackContext, paramsPartition, model);
            if (progressEvent.isFailed()) return progressEvent;
        }
        return ProgressEvent.progress(model, callbackContext);
    }

    private ProgressEvent<ResourceModel, CallbackContext> modifyParameters(final ProxyClient<RdsClient> proxyClient,
                                                                           final AmazonWebServicesClientProxy proxy,
                                                                           final CallbackContext callbackContext,
                                                                           final List<Parameter> paramsPartition,
                                                                           final ResourceModel model) {
        return proxy.initiate("rds::modify-db-cluster-parameter-group", proxyClient, model, callbackContext)
                .translateToServiceRequest((resourceModel) -> Translator.modifyDbClusterParameterGroupRequest(resourceModel, paramsPartition))
                .makeServiceCall((request, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(request, proxyInvocation.client()::modifyDBClusterParameterGroup))
                .handleError((describeDbParameterGroupsRequest, exception, client, resourceModel, ctx) ->
                        Commons.handleException(
                                ProgressEvent.progress(resourceModel, ctx),
                                exception,
                                DEFAULT_DB_CLUSTER_PARAMETER_GROUP_ERROR_RULE_SET))
                .progress();
    }

    protected ProgressEvent<ResourceModel, CallbackContext> waitForDbClustersStabilization(final ProgressEvent<ResourceModel, CallbackContext> progress,
                                                                                           final AmazonWebServicesClientProxy proxy,
                                                                                           final ProxyClient<RdsClient> proxyClient) {
        return proxy.initiate("rds::stabilize-db-clusters-" + getClass().getSimpleName(), proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(Function.identity())
                .makeServiceCall(EMPTY_CALL)
                .stabilize((resourceModel, response, proxyInvocation, model, callbackContext) -> proxyInvocation
                        .injectCredentialsAndInvokeIterableV2(Translator.describeDbClustersRequest(), proxyInvocation.client()::describeDBClustersPaginator)
                        .stream()
                        .flatMap(describeDbClustersResponse -> describeDbClustersResponse.dbClusters().stream())
                        .filter(dbCluster -> dbCluster.dbClusterParameterGroup().equals(model.getDBClusterParameterGroupName()))
                        .allMatch(dbCluster -> dbCluster.status().equals(AVAILABLE)))
                .handleError((describeDbParameterGroupsRequest, exception, client, resourceModel, ctx) ->
                        Commons.handleException(
                                ProgressEvent.progress(resourceModel, ctx),
                                exception,
                                DB_CLUSTERS_STABILIZATION_ERROR_RULE_SET))
                .progress();
    }
}
