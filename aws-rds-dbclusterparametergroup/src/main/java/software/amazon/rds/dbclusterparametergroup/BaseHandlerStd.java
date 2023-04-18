package software.amazon.rds.dbclusterparametergroup;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.BooleanUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.NonNull;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.DbClusterParameterGroupNotFoundException;
import software.amazon.awssdk.services.rds.model.DbParameterGroupAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.DbParameterGroupNotFoundException;
import software.amazon.awssdk.services.rds.model.DbParameterGroupQuotaExceededException;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParameterGroupsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParameterGroupsResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParametersRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParametersResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersResponse;
import software.amazon.awssdk.services.rds.model.DescribeEngineDefaultClusterParametersRequest;
import software.amazon.awssdk.services.rds.model.DescribeEngineDefaultClusterParametersResponse;
import software.amazon.awssdk.services.rds.model.EngineDefaults;
import software.amazon.awssdk.services.rds.model.Filter;
import software.amazon.awssdk.services.rds.model.InvalidDbParameterGroupStateException;
import software.amazon.awssdk.services.rds.model.Parameter;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.CallChain.Completed;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.delay.Constant;
import software.amazon.rds.common.error.ErrorCode;
import software.amazon.rds.common.error.ErrorRuleSet;
import software.amazon.rds.common.error.ErrorStatus;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.Probing;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.common.logging.LoggingProxyClient;
import software.amazon.rds.common.logging.RequestLogger;
import software.amazon.rds.common.printer.FilteredJsonPrinter;
import software.amazon.rds.common.util.ParameterGrouper;

public abstract class BaseHandlerStd extends BaseHandler<CallbackContext> {
    public static final List<Set<String>> PARAMETER_DEPENDENCIES = ImmutableList.of(
            ImmutableSet.of("collation_server", "character_set_server"),
            ImmutableSet.of("gtid-mode", "enforce_gtid_consistency"),
            ImmutableSet.of("password_encryption", "rds.accepted_password_auth_method"),
            ImmutableSet.of("ssl_max_protocol_version", "ssl_min_protocol_version"),
            ImmutableSet.of("rds.change_data_capture_streaming", "binlog_format")
    );
    protected static final BiFunction<ResourceModel, ProxyClient<RdsClient>, ResourceModel> EMPTY_CALL = (model, proxyClient) -> model;
    protected static final String AVAILABLE = "available";
    protected static final String RESOURCE_IDENTIFIER = "dbclusterparametergroup";
    protected static final String STACK_NAME = "rds";

    protected static final int MAX_LENGTH_GROUP_NAME = 255;
    protected static final int MAX_PARAMETERS_PER_REQUEST = 20;
    protected static final int MAX_PARAMETER_FILTER_SIZE = 100;
    protected static final int MAX_DESCRIBE_PAGE_DEPTH = 50;

    protected static final ErrorRuleSet DEFAULT_DB_CLUSTER_PARAMETER_GROUP_ERROR_RULE_SET = ErrorRuleSet
            .extend(Commons.DEFAULT_ERROR_RULE_SET)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.ResourceConflict),
                    InvalidDbParameterGroupStateException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.AlreadyExists),
                    DbParameterGroupAlreadyExistsException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.ServiceLimitExceeded),
                    DbParameterGroupQuotaExceededException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.NotFound),
                    DbClusterParameterGroupNotFoundException.class,
                    DbParameterGroupNotFoundException.class)
            .build();

    protected static final ErrorRuleSet DB_CLUSTERS_STABILIZATION_ERROR_RULE_SET = ErrorRuleSet
            .extend(Commons.DEFAULT_ERROR_RULE_SET)
            .withErrorCodes(ErrorStatus.ignore(),
                    ErrorCode.AccessDeniedException,
                    ErrorCode.AccessDenied,
                    ErrorCode.NotAuthorized)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.NotStabilized),
                    Exception.class)
            .build();

    protected static final ErrorRuleSet SOFT_FAIL_IN_PROGRESS_ERROR_RULE_SET = ErrorRuleSet
            .extend(DEFAULT_DB_CLUSTER_PARAMETER_GROUP_ERROR_RULE_SET)
            .withErrorCodes(ErrorStatus.ignore(OperationStatus.IN_PROGRESS),
                    ErrorCode.AccessDenied,
                    ErrorCode.AccessDeniedException)
            .build();

    private final FilteredJsonPrinter PARAMETERS_FILTER = new FilteredJsonPrinter();

    protected final static HandlerConfig DEFAULT_HANDLER_CONFIG = HandlerConfig.builder()
            .probingEnabled(true)
            .backoff(Constant.of().delay(Duration.ofSeconds(30)).timeout(Duration.ofMinutes(180)).build())
            .build();

    protected HandlerConfig config;

    public BaseHandlerStd(final HandlerConfig config) {
        super();
        this.config = config;
    }

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final Logger logger
    ) {
        final CallbackContext context = callbackContext != null ? callbackContext : new CallbackContext();
        context.setDbClusterParameterGroupArn(Translator.buildClusterParameterGroupArn(request).toString());
        return RequestLogger.handleRequest(
                logger,
                request,
                PARAMETERS_FILTER,
                requestLogger -> handleRequest(proxy,
                        request,
                        context,
                        new LoggingProxyClient<>(requestLogger, proxy.newProxy(new ClientProvider()::getClient)),
                        requestLogger));
    }

    protected abstract ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            AmazonWebServicesClientProxy proxy,
            ResourceHandlerRequest<ResourceModel> request,
            CallbackContext callbackContext,
            ProxyClient<RdsClient> client,
            RequestLogger logger
    );

    protected ProgressEvent<ResourceModel, CallbackContext> updateTags(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Tagging.TagSet previousTags,
            final Tagging.TagSet desiredTags
    ) {
        final Tagging.TagSet tagsToAdd = Tagging.exclude(desiredTags, previousTags);
        final Tagging.TagSet tagsToRemove = Tagging.exclude(previousTags, desiredTags);

        if (tagsToAdd.isEmpty() && tagsToRemove.isEmpty()) {
            return progress;
        }

        try {
            final String arn = progress.getCallbackContext().getDbClusterParameterGroupArn();
            Tagging.removeTags(rdsProxyClient, arn, Tagging.translateTagsToSdk(tagsToRemove));
            Tagging.addTags(rdsProxyClient, arn, Tagging.translateTagsToSdk(tagsToAdd));
        } catch (Exception exception) {
            return Commons.handleException(
                    progress,
                    exception,
                    DEFAULT_DB_CLUSTER_PARAMETER_GROUP_ERROR_RULE_SET.extendWith(
                            Tagging.bestEffortErrorRuleSet(
                                    tagsToAdd,
                                    tagsToRemove,
                                    Tagging.SOFT_FAIL_IN_PROGRESS_TAGGING_ERROR_RULE_SET,
                                    Tagging.HARD_FAIL_TAG_ERROR_RULE_SET
                            )
                    )
            );
        }

        return progress;
    }

    protected ProgressEvent<ResourceModel, CallbackContext> applyParameters(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Map<String, Object> desiredParams,
            final RequestLogger logger
    ) {
        final Map<String, Parameter> currentClusterParameters = Maps.newHashMap();

        final List<String> filterParameterNames = new ArrayList<String>(desiredParams.keySet());

        return ProgressEvent.progress(progress.getResourceModel(), progress.getCallbackContext())
                .then(progressEvent -> describeCurrentDBClusterParameters(proxy, proxyClient, progressEvent, filterParameterNames, currentClusterParameters, logger))
                .then(progressEvent -> validateModelParameters(progressEvent, currentClusterParameters))
                .then(progressEvent -> Commons.execOnce(progressEvent, () -> modifyParameters(progressEvent, currentClusterParameters, proxy, proxyClient),
                        CallbackContext::isParametersModified, CallbackContext::setParametersModified))
                .then(progressEvent -> waitForDbClustersStabilization(progressEvent, proxy, proxyClient));
    }

    protected Completed<DescribeDbClusterParameterGroupsRequest,
            DescribeDbClusterParameterGroupsResponse,
            RdsClient,
            ResourceModel,
            CallbackContext> describeDbClusterParameterGroup(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ResourceModel model,
            final CallbackContext callbackContext
    ) {
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
        final Map<String, Object> modelParameters = Optional.ofNullable(progress.getResourceModel().getParameters()).orElse(Collections.emptyMap());
        final List<String> invalidParameters = modelParameters.keySet().stream()
                .filter(parameterName -> {
                    if (!defaultEngineParameters.containsKey(parameterName)) {
                        return true;
                    }
                    final String newParameterValue = String.valueOf(modelParameters.get(parameterName));
                    final Parameter defaultParameter = defaultEngineParameters.get(parameterName);
                    //Parameter is not modifiable and input model contains different value from default value
                    return newParameterValue != null && BooleanUtils.isNotTrue(defaultParameter.isModifiable())
                            && !newParameterValue.equals(defaultParameter.parameterValue());
                })
                .collect(Collectors.toList());

        if (!invalidParameters.isEmpty()) {
            return ProgressEvent.failed(
                    progress.getResourceModel(),
                    progress.getCallbackContext(),
                    HandlerErrorCode.InvalidRequest,
                    "Invalid / Unmodifiable / Unsupported DB Parameter: " + invalidParameters.stream().findFirst().get());
        }
        return progress;
    }

    private Iterable<Parameter> fetchDBClusterParametersIterable(
            final ProxyClient<RdsClient> proxyClient,
            final DescribeDbClusterParametersRequest request
    ) {
        Iterable<Parameter> result = Collections.emptyList();

        String marker = null;
        int page = 0;
        do {
            if (page >= MAX_DESCRIBE_PAGE_DEPTH) {
                throw new CfnInvalidRequestException("Max DescribeDBParameters page reached.");
            }
            final DescribeDbClusterParametersResponse response = proxyClient.injectCredentialsAndInvokeV2(
                    request.toBuilder().marker(marker).build(),
                    proxyClient.client()::describeDBClusterParameters
            );
            if (response.parameters() != null) {
                result = Iterables.concat(result, response.parameters());
            }
            marker = response.marker();
            page++;
        } while (marker != null);

        return result;
    }

    private Iterable<Parameter> fetchDBClusterParametersIterableWithFilters(
            final ProxyClient<RdsClient> proxyClient,
            final String dbClusterParameterGroupName,
            final List<String> filterParameterNames
    ) {
        Iterable<Parameter> iterable = Collections.emptyList();

        if (filterParameterNames == null) {
            final DescribeDbClusterParametersRequest request = DescribeDbClusterParametersRequest.builder()
                    .dbClusterParameterGroupName(dbClusterParameterGroupName)
                    .build();
            iterable = fetchDBClusterParametersIterable(proxyClient, request);
        } else {
            for (final List<String> partition : Lists.partition(filterParameterNames, MAX_PARAMETER_FILTER_SIZE)) {
                final Filter[] filters = new Filter[]{Translator.filterByParameterNames(partition)};
                final DescribeDbClusterParametersRequest request = DescribeDbClusterParametersRequest.builder()
                        .dbClusterParameterGroupName(dbClusterParameterGroupName)
                        .filters(filters)
                        .build();
                iterable = Iterables.concat(iterable, fetchDBClusterParametersIterable(proxyClient, request));
            }
        }

        return iterable;
    }

    protected ProgressEvent<ResourceModel, CallbackContext> describeCurrentDBClusterParameters(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final List<String> filterParameterNames,
            final Map<String, Parameter> accumulator,
            final RequestLogger logger
    ) {
        try {
            final Iterable<Parameter> parameters = fetchDBClusterParametersIterableWithFilters(
                    proxyClient,
                    progress.getResourceModel().getDBClusterParameterGroupName(),
                    filterParameterNames
            );
            for (final Parameter parameter : parameters) {
                accumulator.put(parameter.parameterName(), parameter);
            }
        } catch (Exception e) {
            return Commons.handleException(progress, e, DEFAULT_DB_CLUSTER_PARAMETER_GROUP_ERROR_RULE_SET);
        }
        return progress;
    }

    private Iterable<Parameter> fetchEngineDefaultClusterParametersIterable(
            final ProxyClient<RdsClient> proxyClient,
            final DescribeEngineDefaultClusterParametersRequest request
    ) {
        Iterable<Parameter> result = Collections.emptyList();

        String marker = null;
        int page = 0;
        do {
            if (page >= MAX_DESCRIBE_PAGE_DEPTH) {
                throw new RuntimeException("Max DescribeEngineDefaultClusterParameters page reached.");
            }
            final DescribeEngineDefaultClusterParametersResponse response = proxyClient.injectCredentialsAndInvokeV2(
                    request.toBuilder().marker(marker).build(),
                    proxyClient.client()::describeEngineDefaultClusterParameters
            );
            final EngineDefaults engineDefaults = response.engineDefaults();
            if (engineDefaults == null) {
                break;
            }
            if (engineDefaults.parameters() != null) {
                result = Iterables.concat(result, engineDefaults.parameters());
            }
            marker = response.engineDefaults().marker();
            page++;
        } while (marker != null);

        return result;
    }

    private Iterable<Parameter> fetchEngineDefaultClusterParametersIterableWithFilters(
            final ProxyClient<RdsClient> proxyClient,
            final String dbParameterGroupFamily,
            final List<String> filterParameterNames
    ) {
        Iterable<Parameter> iterable = Collections.emptyList();

        if (filterParameterNames == null) {
            final DescribeEngineDefaultClusterParametersRequest request = DescribeEngineDefaultClusterParametersRequest.builder()
                    .dbParameterGroupFamily(dbParameterGroupFamily)
                    .build();
            iterable = fetchEngineDefaultClusterParametersIterable(proxyClient, request);
        } else {
            for (final List<String> partition : Lists.partition(filterParameterNames, MAX_PARAMETER_FILTER_SIZE)) {
                final Filter[] filters = new Filter[]{Translator.filterByParameterNames(partition)};
                final DescribeEngineDefaultClusterParametersRequest request = DescribeEngineDefaultClusterParametersRequest.builder()
                        .dbParameterGroupFamily(dbParameterGroupFamily)
                        .filters(filters)
                        .build();
                iterable = Iterables.concat(iterable, fetchEngineDefaultClusterParametersIterable(proxyClient, request));
            }
        }

        return iterable;
    }

    protected ProgressEvent<ResourceModel, CallbackContext> describeEngineDefaultClusterParameters(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final List<String> filterParameterNames,
            final Map<String, Parameter> accumulator,
            final RequestLogger logger
    ) {
        try {
            final Iterable<Parameter> parameters = fetchEngineDefaultClusterParametersIterableWithFilters(
                    proxyClient,
                    progress.getResourceModel().getFamily(),
                    filterParameterNames
            );
            for (final Parameter parameter : parameters) {
                accumulator.put(parameter.parameterName(), parameter);
            }
        } catch (Exception e) {
            return Commons.handleException(progress, e, DEFAULT_DB_CLUSTER_PARAMETER_GROUP_ERROR_RULE_SET);
        }

        return progress;
    }

    private Map<String, Parameter> getParametersToModify(
            final Map<String, Object> modelParameters,
            final Map<String, Parameter> currentDBParameters
    ) {
        final Map<String, Parameter> parametersToModify = Maps.newHashMap(currentDBParameters);
        parametersToModify.keySet().retainAll(Optional.ofNullable(modelParameters).orElse(Collections.emptyMap()).keySet());
        return parametersToModify.entrySet()
                .stream()
                //filter to parameters want to modify and its value is different from already exist value
                .filter(entry -> {
                    final String parameterName = entry.getKey();
                    final String currentParameterValue = entry.getValue().parameterValue();
                    final String newParameterValue = String.valueOf(modelParameters.get(parameterName));
                    return !newParameterValue.equals(currentParameterValue);
                })
                .collect(Collectors.toMap(Map.Entry::getKey,
                        entry -> {
                            final String parameterName = entry.getKey();
                            final String newValue = String.valueOf(modelParameters.get(parameterName));
                            final Parameter defaultParameter = entry.getValue();
                            return Translator.buildParameterWithNewValue(newValue, defaultParameter);
                        })
                );
    }

    @VisibleForTesting
    static Map<String, Parameter> computeModifiedDBParameters(
            @NonNull final Map<String, Parameter> engineDefaultParameters,
            @NonNull final Map<String, Parameter> currentDBParameters
    ) {
        final Map<String, Parameter> modifiedParameters = new HashMap<>();
        for (final String paramName : currentDBParameters.keySet()) {
            final Parameter currentParam = currentDBParameters.get(paramName);
            final Parameter defaultParam = engineDefaultParameters.get(paramName);
            if (defaultParam == null || !Objects.equals(defaultParam.parameterValue(), currentParam.parameterValue())) {
                modifiedParameters.put(paramName, currentParam);
            }
        }

        return modifiedParameters;
    }

    protected ProgressEvent<ResourceModel, CallbackContext> resetAllParameters(
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient
    ) {
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

    private ProgressEvent<ResourceModel, CallbackContext> modifyParameters(
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Map<String, Parameter> currentDBParameters,
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient
    ) {
        final ResourceModel model = progress.getResourceModel();
        final CallbackContext context = progress.getCallbackContext();
        final Map<String, Parameter> parametersToModify = getParametersToModify(model.getParameters(), currentDBParameters);

        try {
            for (final List<Parameter> partition : ParameterGrouper.partition(parametersToModify, PARAMETER_DEPENDENCIES, MAX_PARAMETERS_PER_REQUEST)) {
                proxyClient.injectCredentialsAndInvokeV2(
                        Translator.modifyDbClusterParameterGroupRequest(model, partition),
                        proxyClient.client()::modifyDBClusterParameterGroup
                );
            }
        } catch (Exception exception) {
            return Commons.handleException(ProgressEvent.progress(model, context),
                    exception,
                    DEFAULT_DB_CLUSTER_PARAMETER_GROUP_ERROR_RULE_SET);
        }

        return ProgressEvent.progress(model, context);
    }

    protected boolean isDBClustersAvailable(final ProxyClient<RdsClient> proxyClient, final ResourceModel model) {
        String marker = null;
        int page = 1;
        final DescribeDbClustersRequest request = Translator.describeDbClustersRequestForDBClusterParameterGroup(model);

        do {
            if (page > MAX_DESCRIBE_PAGE_DEPTH) {
                throw new CfnInvalidRequestException("Max describeDBClusters response page reached.");
            }
            final DescribeDbClustersResponse response = proxyClient.injectCredentialsAndInvokeV2(
                    request.toBuilder().marker(marker).build(),
                    proxyClient.client()::describeDBClusters
            );
            final List<DBCluster> dbClusters = Optional.ofNullable(response.dbClusters()).orElse(Collections.emptyList());
            for (final DBCluster dbCluster : dbClusters) {
                if (!AVAILABLE.equalsIgnoreCase(dbCluster.status())) {
                    return false;
                }
            }
            marker = response.marker();
            page++;
        } while (marker != null);

        return true;
    }

    protected ProgressEvent<ResourceModel, CallbackContext> waitForDbClustersStabilization(
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient
    ) {
        return proxy.initiate("rds::stabilize-db-cluster-parameter-group-db-clusters", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(Function.identity())
                .backoffDelay(config.getBackoff())
                .makeServiceCall(EMPTY_CALL)
                .stabilize((request, response, proxyInvocation, model, context) -> Probing.withProbing(context.getProbingContext(),
                        "db-cluster-parameter-group-db-clusters-available",
                        3,
                        () -> isDBClustersAvailable(proxyInvocation, model)))
                .handleError((describeDbParameterGroupsRequest, exception, client, resourceModel, ctx) ->
                        Commons.handleException(
                                ProgressEvent.progress(resourceModel, ctx),
                                exception,
                                DB_CLUSTERS_STABILIZATION_ERROR_RULE_SET))
                .progress();
    }
}
