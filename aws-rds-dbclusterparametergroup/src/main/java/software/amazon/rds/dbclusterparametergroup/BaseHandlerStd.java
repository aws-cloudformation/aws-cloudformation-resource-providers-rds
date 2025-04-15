package software.amazon.rds.dbclusterparametergroup;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.NonNull;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBClusterParameterGroup;
import software.amazon.awssdk.services.rds.model.DbClusterParameterGroupNotFoundException;
import software.amazon.awssdk.services.rds.model.DbParameterGroupAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.DbParameterGroupNotFoundException;
import software.amazon.awssdk.services.rds.model.DbParameterGroupQuotaExceededException;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParameterGroupsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParameterGroupsResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParametersRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParametersResponse;
import software.amazon.awssdk.services.rds.model.DescribeEngineDefaultClusterParametersRequest;
import software.amazon.awssdk.services.rds.model.DescribeEngineDefaultClusterParametersResponse;
import software.amazon.awssdk.services.rds.model.EngineDefaults;
import software.amazon.awssdk.services.rds.model.Filter;
import software.amazon.awssdk.services.rds.model.InvalidDbParameterGroupStateException;
import software.amazon.awssdk.services.rds.model.Parameter;
import software.amazon.awssdk.services.rds.model.Tag;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
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
            ImmutableSet.of("rds.change_data_capture_streaming", "binlog_format"),
            ImmutableSet.of("aurora_enhanced_binlog", "binlog_backup", "binlog_replication_globaldb")
    );
    protected static final BiFunction<ResourceModel, ProxyClient<RdsClient>, ResourceModel> EMPTY_CALL = (model, proxyClient) -> model;
    protected static final String AVAILABLE = "available";
    protected static final String RESOURCE_IDENTIFIER = "dbclusterparametergroup";
    protected static final String STACK_NAME = "rds";
    protected static final String DB_CLUSTER_PARAMETER_GROUP_REQUEST_STARTED_AT = "dbclusterparametergroup-request-started-at";
    protected static final String DB_CLUSTER_PARAMETER_GROUP_REQUEST_IN_PROGRESS_AT = "dbclusterparametergroup-request-in-progress-at";
    protected static final String DB_CLUSTER_PARAMETER_GROUP_RESOURCE_STABILIZATION_TIME = "dbclusterparametergroup-stabilization-time";

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
    protected RequestLogger requestLogger;

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
                        new LoggingProxyClient<>(requestLogger, proxy.newProxy(new ClientProvider()::getClient)), request,
                        context, requestLogger
                ));
    }

    protected abstract ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            AmazonWebServicesClientProxy proxy,
            ProxyClient<RdsClient> client,
            ResourceHandlerRequest<ResourceModel> request,
            CallbackContext callbackContext
    );

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final RequestLogger requestLogger
    ) {
        this.requestLogger = requestLogger;
        resourceStabilizationTime(callbackContext);
        return handleRequest(proxy, proxyClient, request, callbackContext);
    }

    protected ProgressEvent<ResourceModel, CallbackContext> updateTags(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Tagging.TagSet previousTags,
            final Tagging.TagSet desiredTags
    ) {
        final Collection<Tag> effectivePreviousTags = Tagging.translateTagsToSdk(previousTags);
        final Collection<Tag> effectiveDesiredTags = Tagging.translateTagsToSdk(desiredTags);

        final Collection<Tag> tagsToRemove = Tagging.exclude(effectivePreviousTags, effectiveDesiredTags);
        final Collection<Tag> tagsToAdd = Tagging.exclude(effectiveDesiredTags, effectivePreviousTags);

        if (tagsToAdd.isEmpty() && tagsToRemove.isEmpty()) {
            return progress;
        }

        final Tagging.TagSet rulesetTagsToAdd = Tagging.exclude(desiredTags, previousTags);
        final Tagging.TagSet rulesetTagsToRemove = Tagging.exclude(previousTags, desiredTags);

        try {
            final String arn = progress.getCallbackContext().getDbClusterParameterGroupArn();
            Tagging.removeTags(rdsProxyClient, arn, Tagging.translateTagsToSdk(tagsToRemove));
            Tagging.addTags(rdsProxyClient, arn, Tagging.translateTagsToSdk(tagsToAdd));
        } catch (Exception exception) {
            return Commons.handleException(
                    progress,
                    exception,
                    DEFAULT_DB_CLUSTER_PARAMETER_GROUP_ERROR_RULE_SET.extendWith(
                            Tagging.getUpdateTagsAccessDeniedRuleSet(
                                    rulesetTagsToAdd,
                                    rulesetTagsToRemove
                            )
                    ),
                    requestLogger
            );
        }

        return progress;
    }

    protected ProgressEvent<ResourceModel, CallbackContext> applyParameters(
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Map<String, Parameter> currentClusterParameters,
            final Map<String, Parameter> desiredClusterParameters
    ) {
        return ProgressEvent.progress(progress.getResourceModel(), progress.getCallbackContext())
            .then(progressEvent -> validateModelParameters(progressEvent, currentClusterParameters, desiredClusterParameters))
            .then(progressEvent -> Commons.execOnce(progressEvent, () -> modifyParameters(progressEvent, proxyClient, currentClusterParameters, desiredClusterParameters),
                CallbackContext::isParametersModified, CallbackContext::setParametersModified));
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
                        SOFT_FAIL_IN_PROGRESS_ERROR_RULE_SET,
                        requestLogger));
    }

    /**
     * This function validates that all the parameters we want to modify can be modified
     * A parameter is considered invalid to be modified, if it is both not modifiable, and the value has actually changed
     *
     * @param progress CloudFormation progress event
     * @param currentClusterParameters Cluster parameters currently attached to the physical Cluster Parameter Group Resource
     * @param desiredClusterParameters Cluster parameters which are desired, and we wish to apply
     * @return CloudFormation progress event
     */
    private ProgressEvent<ResourceModel, CallbackContext> validateModelParameters(
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Map<String, Parameter> currentClusterParameters,
            final Map<String, Parameter> desiredClusterParameters
    ) {
        // we don't care about putAll overwriting overlapping keys, because we just need the map
        // for the isModifiable field
        final Map<String, Parameter> combinedParams = Maps.newHashMap();
        combinedParams.putAll(currentClusterParameters);
        combinedParams.putAll(desiredClusterParameters);

        final Map<String, Object> modelParameters = Optional.ofNullable(progress.getResourceModel().getParameters()).orElse(Collections.emptyMap());
        final List<String> invalidParameters = modelParameters.keySet().stream()
                .filter(modelParameterName -> {
                    final String newParameterValue = String.valueOf(modelParameters.get(modelParameterName));
                    final Parameter currentParameterValue = currentClusterParameters.get(modelParameterName);

                    if (combinedParams.get(modelParameterName) == null) {
                        // it should not go in here, because combinedParams should contain all
                        // current and desired parameters
                        requestLogger.log("DBClusterParameters did not contain %s", modelParameterName);
                        return true;
                    }

                    final boolean isParameterModifiable = combinedParams.get(modelParameterName).isModifiable();

                    // if the parameter is not currently set on our parameter group
                    // and the parameter is not modifiable, it means it is an invalid change
                    if (currentParameterValue == null) {
                        return !isParameterModifiable;
                    }
                    // Parameter is not modifiable and the desired value is different to the current value
                    return !isParameterModifiable && !newParameterValue.equals(currentParameterValue.parameterValue());
                })
                .toList();

        if (!invalidParameters.isEmpty()) {
            return ProgressEvent.failed(
                    progress.getResourceModel(),
                    progress.getCallbackContext(),
                    HandlerErrorCode.InvalidRequest,
                    "Invalid / Unmodifiable / Unsupported DB Parameter: " + invalidParameters.stream().findFirst().get());
        }
        return progress;
    }

    protected DBClusterParameterGroup fetchDBClusterParameterGroup(
            final ProxyClient<RdsClient> proxyClient,
            final ResourceModel model) {
        try {
            final var response = proxyClient.injectCredentialsAndInvokeV2(Translator.describeDbClusterParameterGroupsRequest(model), proxyClient.client()::describeDBClusterParameterGroups);
            if (!response.hasDbClusterParameterGroups() || response.dbClusterParameterGroups().isEmpty()) {
                throw new CfnNotFoundException(ResourceModel.TYPE_NAME, model.getDBClusterParameterGroupName());
            }
            return response.dbClusterParameterGroups().get(0);
        } catch (DbParameterGroupNotFoundException e) {
            // !!!: DescribeDBClusterParameterGroups throws DbParameterGroupNotFound, NOT DBClusterParameterGroupNotFound!
            throw new CfnNotFoundException(e);
        }
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

    protected ProgressEvent<ResourceModel, CallbackContext> describeDBClusterParameters(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final List<String> filterParameterNames,
            final Map<String, Parameter> accumulator
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
            return Commons.handleException(progress, e, DEFAULT_DB_CLUSTER_PARAMETER_GROUP_ERROR_RULE_SET, requestLogger);
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
            final Map<String, Parameter> accumulator
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
            return Commons.handleException(progress, e, DEFAULT_DB_CLUSTER_PARAMETER_GROUP_ERROR_RULE_SET, requestLogger);
        }

        return progress;
    }

    private Map<String, Parameter> getParametersToModify(
            final Map<String, Object> modelParameters,
            final Map<String, Parameter> currentDBParameters,
            final Map<String, Parameter> desiredDBParameters
    ) {
        final Map<String, Parameter> parametersToModify = Maps.newHashMap(desiredDBParameters);

        return parametersToModify.entrySet()
                .stream()
                //filter to parameters want to modify and its value is different from already exist value
                .filter(entry -> {
                    final String parameterName = entry.getKey();
                    final String currentParameterValue = currentDBParameters.get(entry.getKey()) != null ? currentDBParameters.get(entry.getKey()).parameterValue() : null;
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

    /**
     * Checks if parameter has been modified out of band and no longer matches
     * the CloudFormation model. This can happen if the parameter is modified
     * directly in the AWS console or API.
     *
     * @return True if the parameter has been overridden out of band and the
     * physical resource no longer matching with the CloudFormation model
     */
    private boolean isParameterModifiedOutOfBand(final String prevModelParamValue, final String currentParameterValue) {
        return !prevModelParamValue.equals(currentParameterValue);
    }

    protected ProgressEvent<ResourceModel, CallbackContext> resetParameters(
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final Map<String, Object> previousModelParams,
            final Map<String, Parameter> currentParameters,
            final Map<String, Parameter> desiredParameters
    ) {
        // reset only parameters that are missing from desired params. If parameter is in desired param, its value will be updated anyway.
        final Map<String, Parameter> toBeReset = currentParameters.keySet().stream()
                .filter(p -> {
                    return !desiredParameters.containsKey(p) &&
                        !isParameterModifiedOutOfBand(previousModelParams.get(p).toString(), currentParameters.get(p).parameterValue());
                })
                .collect(Collectors.toMap(kv -> kv, currentParameters::get));

        if (toBeReset.isEmpty()) {
            return progress;
        }

        return proxy.initiate("rds::reset-db-cluster-parameter-group", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(m -> Translator.resetDbClusterParameterGroupRequest(m, toBeReset))
                .makeServiceCall((request, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(request, proxyInvocation.client()::resetDBClusterParameterGroup))
                .handleError((resetDbClusterParameterGroupRequest, exception, client, resourceModel, ctx) ->
                        Commons.handleException(
                                ProgressEvent.progress(resourceModel, ctx),
                                exception,
                                DEFAULT_DB_CLUSTER_PARAMETER_GROUP_ERROR_RULE_SET,
                                requestLogger))
                .progress();
    }

    private ProgressEvent<ResourceModel, CallbackContext> modifyParameters(
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final ProxyClient<RdsClient> proxyClient,
            final Map<String, Parameter> currentDBParameters,
            final Map<String, Parameter> desiredDBParameters
    ) {
        final ResourceModel model = progress.getResourceModel();
        final CallbackContext context = progress.getCallbackContext();
        final Map<String, Parameter> parametersToModify = getParametersToModify(model.getParameters(), currentDBParameters, desiredDBParameters);

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
                    DEFAULT_DB_CLUSTER_PARAMETER_GROUP_ERROR_RULE_SET,
                    requestLogger);
        }

        return ProgressEvent.progress(model, context);
    }

    private void resourceStabilizationTime(final CallbackContext callbackContext) {
        callbackContext.timestampOnce(DB_CLUSTER_PARAMETER_GROUP_REQUEST_STARTED_AT, Instant.now());
        callbackContext.timestamp(DB_CLUSTER_PARAMETER_GROUP_REQUEST_IN_PROGRESS_AT, Instant.now());
        callbackContext.calculateTimeDeltaInMinutes(DB_CLUSTER_PARAMETER_GROUP_RESOURCE_STABILIZATION_TIME,
                callbackContext.getTimestamp(DB_CLUSTER_PARAMETER_GROUP_REQUEST_IN_PROGRESS_AT),
                callbackContext.getTimestamp(DB_CLUSTER_PARAMETER_GROUP_REQUEST_STARTED_AT));
    }
}
