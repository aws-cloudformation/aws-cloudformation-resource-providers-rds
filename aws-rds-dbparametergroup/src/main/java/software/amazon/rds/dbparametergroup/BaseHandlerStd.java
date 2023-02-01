package software.amazon.rds.dbparametergroup;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DbParameterGroupAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.DbParameterGroupNotFoundException;
import software.amazon.awssdk.services.rds.model.DbParameterGroupQuotaExceededException;
import software.amazon.awssdk.services.rds.model.DescribeDbParametersResponse;
import software.amazon.awssdk.services.rds.model.DescribeEngineDefaultParametersResponse;
import software.amazon.awssdk.services.rds.model.InvalidDbParameterGroupStateException;
import software.amazon.awssdk.services.rds.model.Parameter;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
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
import software.amazon.rds.common.util.ParameterGrouper;

public abstract class BaseHandlerStd extends BaseHandler<CallbackContext> {
    public static final List<Set<String>> DEPENDENCIES = ImmutableList.of(
            ImmutableSet.of("collation_server", "character_set_server"),
            ImmutableSet.of("gtid-mode", "enforce_gtid_consistency"),
            ImmutableSet.of("password_encryption", "rds.accepted_password_auth_method"),
            ImmutableSet.of("ssl_max_protocol_version", "ssl_min_protocol_version")
    );

    protected static final ErrorRuleSet DEFAULT_DB_PARAMETER_GROUP_ERROR_RULE_SET = ErrorRuleSet
            .extend(Commons.DEFAULT_ERROR_RULE_SET)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.ResourceConflict),
                    InvalidDbParameterGroupStateException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.AlreadyExists),
                    DbParameterGroupAlreadyExistsException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.NotFound),
                    DbParameterGroupNotFoundException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.ServiceLimitExceeded),
                    DbParameterGroupQuotaExceededException.class)
            .build();

    protected static final ErrorRuleSet SOFT_FAIL_IN_PROGRESS_TAGGING_ERROR_RULE_SET = ErrorRuleSet
            .extend(DEFAULT_DB_PARAMETER_GROUP_ERROR_RULE_SET)
            .withErrorCodes(ErrorStatus.ignore(OperationStatus.IN_PROGRESS),
                    ErrorCode.AccessDenied,
                    ErrorCode.AccessDeniedException)
            .build();

    protected static final int MAX_LENGTH_GROUP_NAME = 255;
    protected static final int NO_CALLBACK_DELAY = 0;
    protected static final int MAX_PARAMETERS_PER_REQUEST = 20;

    protected static final int MAX_PARAMETER_FILTER_SIZE = 100;
    protected static final int MAX_PARAMETER_DESCRIBE_DEPTH = 20;

    protected static final String RESOURCE_IDENTIFIER = "dbparametergroup";
    protected static final String STACK_NAME = "rds";

    protected HandlerConfig config;

    private final FilteredJsonPrinter PARAMETERS_FILTER = new FilteredJsonPrinter();

    public BaseHandlerStd(final HandlerConfig config) {
        super();
        this.config = config;
    }

    @Override
    public final ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final Logger logger
    ) {
        final CallbackContext context = callbackContext != null ? callbackContext : new CallbackContext();
        context.setDbParameterGroupArn(Translator.buildParameterGroupArn(request).toString());
        return RequestLogger.handleRequest(
                logger,
                request,
                PARAMETERS_FILTER,
                requestLogger -> handleRequest(
                        proxy,
                        new LoggingProxyClient<>(requestLogger, proxy.newProxy(new ClientProvider()::getClient)),
                        request,
                        context,
                        requestLogger
                )
        );
    }

    protected abstract ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final RequestLogger logger
    );

    protected ProgressEvent<ResourceModel, CallbackContext> updateTags(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Tagging.TagSet previousTags,
            final Tagging.TagSet desiredTags,
            final RequestLogger logger
    ) {
        final Tagging.TagSet tagsToAdd = Tagging.exclude(desiredTags, previousTags);
        final Tagging.TagSet tagsToRemove = Tagging.exclude(previousTags, desiredTags);

        if (tagsToAdd.isEmpty() && tagsToRemove.isEmpty()) {
            return progress;
        }

        try {
            final String arn = progress.getCallbackContext().getDbParameterGroupArn();
            Tagging.removeTags(rdsProxyClient, arn, Tagging.translateTagsToSdk(tagsToRemove));
            Tagging.addTags(rdsProxyClient, arn, Tagging.translateTagsToSdk(tagsToAdd));
        } catch (Exception exception) {
            return Commons.handleException(
                    progress,
                    exception,
                    DEFAULT_DB_PARAMETER_GROUP_ERROR_RULE_SET.extendWith(
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

    protected ProgressEvent<ResourceModel, CallbackContext> applyParametersWithReset(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Map<String, Object> previousParams,
            final Map<String, Object> desiredParams,
            final RequestLogger logger
    ) {
        final ResourceModel model = progress.getResourceModel();
        final CallbackContext context = progress.getCallbackContext();

        //isParametersApplied flag for unit testing to skip these calls
        if (context.isParametersApplied()) {
            return ProgressEvent.defaultInProgressHandler(context, NO_CALLBACK_DELAY, model);
        }

        //These containers will be populated in upcoming calls in progress chain.
        final Map<String, Parameter> defaultParams = Maps.newHashMap();
        final Map<String, Parameter> currentParams = Maps.newHashMap();

        final Set<String> paramNames = new HashSet<>();
        paramNames.addAll(Optional.ofNullable(previousParams).orElse(Collections.emptyMap()).keySet());
        paramNames.addAll(Optional.ofNullable(desiredParams).orElse(Collections.emptyMap()).keySet());

        if (paramNames.isEmpty()) {
            return progress;
        }

        return ProgressEvent.progress(model, context)
                .then(p -> describeDefaultEngineParameters(proxy, proxyClient, p, new ArrayList<>(paramNames), defaultParams, logger))
                .then(p -> validateModelParameters(p, defaultParams, logger))
                .then(p -> describeCurrentDBParameters(p, new ArrayList<>(paramNames), currentParams, proxy, proxyClient, logger))
                .then(p -> resetParameters(p, defaultParams, currentParams, proxy, proxyClient, logger))
                .then(p -> modifyParameters(proxy, proxyClient, p, currentParams, logger));
    }

    protected ProgressEvent<ResourceModel, CallbackContext> applyParameters(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Map<String, Object> desiredParams,
            final RequestLogger logger
    ) {
        final ResourceModel model = progress.getResourceModel();
        final CallbackContext callbackContext = progress.getCallbackContext();
        //isParametersApplied flag for unit testing to skip these calls
        if (callbackContext.isParametersApplied()) {
            return ProgressEvent.defaultInProgressHandler(callbackContext, NO_CALLBACK_DELAY, model);
        }
        //Map will be populated in upcoming calls in progress chain.
        final Map<String, Parameter> defaultParams = Maps.newHashMap();
        final Set<String> paramNames = new HashSet<>(Optional.ofNullable(desiredParams).orElse(Collections.emptyMap()).keySet());

        if (paramNames.isEmpty()) {
            return progress;
        }

        return ProgressEvent.progress(model, callbackContext)
                .then(p -> describeDefaultEngineParameters(proxy, proxyClient, p, new ArrayList<>(paramNames), defaultParams, logger))
                .then(p -> validateModelParameters(p, defaultParams, logger))
                .then(p -> modifyParameters(proxy, proxyClient, p, defaultParams, logger));
    }

    private ProgressEvent<ResourceModel, CallbackContext> resetParameters(
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Map<String, Parameter> defaultParams,
            final Map<String, Parameter> currentParams,
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final RequestLogger logger
    ) {
        final ResourceModel model = progress.getResourceModel();
        final CallbackContext context = progress.getCallbackContext();
        final Map<String, Parameter> paramsToReset = getParametersToReset(model, defaultParams, currentParams);

        logger.log("ResetParameters", paramsToReset);
        for (final List<Parameter> paramsPartition : ParameterGrouper.partition(paramsToReset, DEPENDENCIES, MAX_PARAMETERS_PER_REQUEST)) {  //modify api call is limited to 20 parameter per request
            final ProgressEvent<ResourceModel, CallbackContext> progressEvent = resetParameters(proxy, model, context, paramsPartition, proxyClient, logger);
            if (progressEvent.isFailed()) {
                return progressEvent;
            }
        }

        return progress;
    }

    private ProgressEvent<ResourceModel, CallbackContext> modifyParameters(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Map<String, Parameter> currentParams,
            final RequestLogger logger
    ) {
        final ResourceModel model = progress.getResourceModel();
        final CallbackContext context = progress.getCallbackContext();

        final Map<String, Parameter> paramsToModify = getModifiableParameters(model, currentParams);

        for (final List<Parameter> paramsPartition : ParameterGrouper.partition(paramsToModify, DEPENDENCIES, MAX_PARAMETERS_PER_REQUEST)) {  //modify api call is limited to 20 parameter per request
            final ProgressEvent<ResourceModel, CallbackContext> progressEvent = modifyParameterGroup(proxy, proxyClient, model, context, paramsPartition, logger);
            if (progressEvent.isFailed()) {
                return progressEvent;
            }
        }

        return progress;
    }

    private ProgressEvent<ResourceModel, CallbackContext> modifyParameterGroup(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ResourceModel model,
            final CallbackContext context,
            final List<Parameter> paramsPartition,
            final RequestLogger logger
    ) {
        return proxy.initiate("rds::modify-db-parameter-group", proxyClient, model, context)
                .translateToServiceRequest((resourceModel) -> Translator.modifyDbParameterGroupRequest(resourceModel, paramsPartition))
                .makeServiceCall((request, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(request, proxyInvocation.client()::modifyDBParameterGroup))
                .handleError((request, exception, client, resourceModel, ctx) ->
                        Commons.handleException(
                                ProgressEvent.progress(resourceModel, ctx),
                                exception,
                                DEFAULT_DB_PARAMETER_GROUP_ERROR_RULE_SET
                        ))
                .progress();
    }

    private ProgressEvent<ResourceModel, CallbackContext> resetParameters(
            final AmazonWebServicesClientProxy proxy,
            final ResourceModel model,
            final CallbackContext context,
            final List<Parameter> paramsPartition,
            final ProxyClient<RdsClient> proxyClient,
            final RequestLogger logger
    ) {
        return proxy.initiate("rds::reset-db-parameter-group", proxyClient, model, context)
                .translateToServiceRequest((resourceModel) -> Translator.resetDbParametersRequest(resourceModel, paramsPartition))
                .makeServiceCall((request, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(request, proxyInvocation.client()::resetDBParameterGroup))
                .handleError((request, exception, client, resourceModel, ctx) ->
                        Commons.handleException(
                                ProgressEvent.progress(resourceModel, ctx),
                                exception,
                                DEFAULT_DB_PARAMETER_GROUP_ERROR_RULE_SET
                        ))
                .progress();
    }

    private Map<String, Parameter> getModifiableParameters(
            final ResourceModel model,
            final Map<String, Parameter> currentParams
    ) {
        final Map<String, Parameter> paramsToModify = Maps.newHashMap(currentParams);
        final Map<String, Object> modelParams = Optional.ofNullable(model.getParameters()).orElse(Collections.emptyMap());
        paramsToModify.keySet().retainAll(modelParams.keySet());

        return paramsToModify.entrySet()
                .stream()
                //filter to parameters want to modify and its value is different from already exist value
                .filter(entry -> {
                    final String name = entry.getKey();
                    final String previousValue = entry.getValue().parameterValue();
                    final String desiredValue = String.valueOf(modelParams.get(name));
                    return !desiredValue.equals(previousValue);
                })
                .collect(Collectors.toMap(Map.Entry::getKey,
                        entry -> {
                            final String parameterName = entry.getKey();
                            final String desiredValue = String.valueOf(modelParams.get(parameterName));
                            final Parameter defaultParam = entry.getValue();
                            return Translator.buildParameterWithNewValue(desiredValue, defaultParam);
                        })
                );
    }

    private ProgressEvent<ResourceModel, CallbackContext> validateModelParameters(
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Map<String, Parameter> defaultParams,
            final RequestLogger requestLogger
    ) {
        final Map<String, Object> modelParams = Optional.ofNullable(progress.getResourceModel().getParameters()).orElse(Collections.emptyMap());

        final Set<String> invalidParams = modelParams.entrySet().stream()
                .filter(entry -> {
                    final String name = entry.getKey();
                    final String value = String.valueOf(entry.getValue());
                    if (!defaultParams.containsKey(name)) {
                        return true;
                    }
                    final Parameter defaultParam = defaultParams.get(name);
                    //Parameter is not modifiable and input model contains different value from default value
                    return value != null && !defaultParam.isModifiable() && !defaultParam.parameterValue().equals(value);
                })
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());

        if (!invalidParams.isEmpty()) {
            requestLogger.log("InvalidParameters", invalidParams);
            return ProgressEvent.failed(
                    progress.getResourceModel(),
                    progress.getCallbackContext(),
                    HandlerErrorCode.InvalidRequest,
                    "Invalid / Unmodifiable / Unsupported DB Parameter: " + invalidParams.stream().findFirst().get());
        }

        return ProgressEvent.progress(progress.getResourceModel(), progress.getCallbackContext());
    }

    private Map<String, Parameter> getParametersToReset(
            final ResourceModel model,
            final Map<String, Parameter> defaultParams,
            final Map<String, Parameter> currentParams
    ) {
        final Map<String, Object> paramsToModify = model.getParameters();
        final Map<String, Parameter> paramsToReset = Maps.newLinkedHashMap(defaultParams);
        paramsToReset.keySet().retainAll(currentParams.keySet());

        return paramsToReset.entrySet()
                .stream()
                .filter(entry -> {
                    final String name = entry.getKey();
                    final String previousValue = entry.getValue().parameterValue();
                    final String desiredValue = currentParams.get(name).parameterValue();
                    return paramsToModify != null && desiredValue != null
                            && !desiredValue.equals(previousValue)
                            && !paramsToModify.containsKey(name);
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private ProgressEvent<ResourceModel, CallbackContext> describeCurrentDBParameters(
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final List<String> paramNames,
            final Map<String, Parameter> currentParams,
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final RequestLogger logger
    ) {
        for (final List<String> paramNamePartition : Lists.partition(paramNames, MAX_PARAMETER_FILTER_SIZE)) {
            String marker = null;
            try {
                int page = 1;
                do {
                    if (page > MAX_PARAMETER_DESCRIBE_DEPTH) {
                        return ProgressEvent.failed(
                                progress.getResourceModel(),
                                progress.getCallbackContext(),
                                HandlerErrorCode.InvalidRequest,
                                "Max DescribeDbParameters response page reached."
                        );
                    }
                    final DescribeDbParametersResponse response = fetchDbParameters(proxyClient, progress.getResourceModel(), paramNamePartition, marker);
                    for (final Parameter parameter : response.parameters()) {
                        currentParams.put(parameter.parameterName(), parameter);
                    }
                    marker = response.marker();
                    page++;
                } while (marker != null);
            } catch (Exception e) {
                return Commons.handleException(progress, e, DEFAULT_DB_PARAMETER_GROUP_ERROR_RULE_SET);
            }
        }
        return progress;
    }

    private ProgressEvent<ResourceModel, CallbackContext> describeDefaultEngineParameters(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final List<String> paramNames,
            final Map<String, Parameter> defaultParams,
            final RequestLogger logger
    ) {
        for (final List<String> paramNamePartition : Lists.partition(paramNames, MAX_PARAMETER_FILTER_SIZE)) {
            String marker = null;
            try {
                int page = 1;
                do {
                    if (page > MAX_PARAMETER_DESCRIBE_DEPTH) {
                        return ProgressEvent.failed(
                                progress.getResourceModel(),
                                progress.getCallbackContext(),
                                HandlerErrorCode.InvalidRequest,
                                "Max DescribeEngineDefaultParameters response page reached."
                        );
                    }
                    final DescribeEngineDefaultParametersResponse response = fetchEngineDefaultParameters(proxyClient, progress.getResourceModel(), paramNamePartition, marker);
                    for (final Parameter parameter : response.engineDefaults().parameters()) {
                        defaultParams.put(parameter.parameterName(), parameter);
                    }
                    marker = response.engineDefaults().marker();
                    page++;
                } while (marker != null);
            } catch (Exception e) {
                return Commons.handleException(progress, e, DEFAULT_DB_PARAMETER_GROUP_ERROR_RULE_SET);
            }
        }
        return progress;
    }

    private DescribeDbParametersResponse fetchDbParameters(
            final ProxyClient<RdsClient> proxyClient,
            final ResourceModel model,
            final List<String> parameterNames,
            final String marker
    ) {
        return proxyClient.injectCredentialsAndInvokeV2(
                Translator.describeDbParametersRequest(model.getDBParameterGroupName(), parameterNames, marker),
                proxyClient.client()::describeDBParameters
        );
    }

    private DescribeEngineDefaultParametersResponse fetchEngineDefaultParameters(
            final ProxyClient<RdsClient> proxyClient,
            final ResourceModel model,
            final List<String> parameterNames,
            final String marker
    ) {
        return proxyClient.injectCredentialsAndInvokeV2(
                Translator.describeEngineDefaultParametersRequest(model.getFamily(), parameterNames, marker),
                proxyClient.client()::describeEngineDefaultParameters
        );
    }
}
