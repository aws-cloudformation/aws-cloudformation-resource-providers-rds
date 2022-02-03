package software.amazon.rds.dbparametergroup;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DbParameterGroupAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.DbParameterGroupNotFoundException;
import software.amazon.awssdk.services.rds.model.DbParameterGroupQuotaExceededException;
import software.amazon.awssdk.services.rds.model.InvalidDbParameterGroupStateException;
import software.amazon.awssdk.services.rds.model.Parameter;
import software.amazon.cloudformation.exceptions.CfnAccessDeniedException;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.delay.Constant;
import software.amazon.rds.common.error.ErrorRuleSet;
import software.amazon.rds.common.error.ErrorStatus;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.logging.RequestLogger;
import software.amazon.rds.common.printer.FilteredJsonPrinter;

public abstract class BaseHandlerStd extends BaseHandler<CallbackContext> {
    protected static final Constant CONSTANT = Constant.of().timeout(Duration.ofMinutes(120L))
            .delay(Duration.ofSeconds(30L)).build();
    protected static final ErrorRuleSet DEFAULT_DB_PARAMETER_GROUP_ERROR_RULE_SET = ErrorRuleSet.builder()
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.ResourceConflict),
                    InvalidDbParameterGroupStateException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.AlreadyExists),
                    DbParameterGroupAlreadyExistsException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.NotFound),
                    DbParameterGroupNotFoundException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.ServiceLimitExceeded),
                    DbParameterGroupQuotaExceededException.class)
            .build()
            .orElse(Commons.DEFAULT_ERROR_RULE_SET);
    protected static final ErrorRuleSet SOFT_FAIL_TAG_DB_PARAMETER_GROUP_ERROR_RULE_SET = ErrorRuleSet.builder()
            .withErrorClasses(ErrorStatus.ignore(), CfnAccessDeniedException.class)
            .build()
            .orElse(DEFAULT_DB_PARAMETER_GROUP_ERROR_RULE_SET);
    protected static int MAX_LENGTH_GROUP_NAME = 255;
    protected static final int NO_CALLBACK_DELAY = 0;
    protected static final int MAX_PARAMETERS_PER_REQUEST = 20;

    private final FilteredJsonPrinter PARAMETERS_FILTER = new FilteredJsonPrinter();

    @Override
    public final ProgressEvent<ResourceModel, CallbackContext> handleRequest(final AmazonWebServicesClientProxy proxy,
                                                                             final ResourceHandlerRequest<ResourceModel> request,
                                                                             final CallbackContext callbackContext,
                                                                             final Logger logger) {
        return RequestLogger.handleRequest(
                logger,
                request,
                PARAMETERS_FILTER,
                requestLogger -> handleRequest(proxy,
                        request,
                        callbackContext != null ? callbackContext : new CallbackContext(),
                        proxy.newProxy(ClientBuilder::getClient),
                        requestLogger));
    }

    protected ProgressEvent<ResourceModel, CallbackContext> applyParameters(final AmazonWebServicesClientProxy proxy,
                                                                            final ProxyClient<RdsClient> proxyClient,
                                                                            final ResourceModel model,
                                                                            final CallbackContext callbackContext,
                                                                            final RequestLogger requestLogger) {
        //isParametersApplied flag for unit testing
        if (callbackContext.isParametersApplied())
            return ProgressEvent.defaultInProgressHandler(callbackContext, NO_CALLBACK_DELAY, model);
        callbackContext.setParametersApplied(true);

        Map<String, Parameter> defaultEngineParameters = Maps.newHashMap();
        Map<String, Parameter> currentDBParameters = Maps.newHashMap();

        return ProgressEvent.progress(model, callbackContext)
                .then(progressEvent -> describeDefaultEngineParameters(progressEvent, defaultEngineParameters, proxy, proxyClient, requestLogger))
                .then(progressEvent -> validateModelParameters(progressEvent, defaultEngineParameters, requestLogger))
                .then(progressEvent -> describeCurrentDBParameters(progressEvent, currentDBParameters, proxy, proxyClient, requestLogger))
                .then(progressEvent -> resetParameters(progressEvent, defaultEngineParameters, currentDBParameters, proxy, proxyClient, requestLogger))
                .then(progressEvent -> modifyParameters(progressEvent, currentDBParameters, proxy, proxyClient, requestLogger));
    }

    private ProgressEvent<ResourceModel, CallbackContext> resetParameters(final ProgressEvent<ResourceModel, CallbackContext> progress,
                                                                          final Map<String, Parameter> defaultEngineParameters,
                                                                          final Map<String, Parameter> currentDBParameters,
                                                                          final AmazonWebServicesClientProxy proxy,
                                                                          final ProxyClient<RdsClient> proxyClient,
                                                                          final RequestLogger requestLogger) {
        ResourceModel model = progress.getResourceModel();
        CallbackContext callbackContext = progress.getCallbackContext();
        Map<String, Parameter> parametersToReset = getParametersToReset(model, defaultEngineParameters, currentDBParameters);
        for (List<Parameter> paramsPartition : Iterables.partition(parametersToReset.values(), MAX_PARAMETERS_PER_REQUEST)) {  //modify api call is limited to 20 parameter per request
            ProgressEvent<ResourceModel, CallbackContext> progressEvent = resetParameters(proxy, model, callbackContext, paramsPartition, proxyClient, requestLogger);
            if (progressEvent.isFailed()) return progressEvent;
        }
        requestLogger.log("ResetParameters", parametersToReset);
        return ProgressEvent.progress(model, callbackContext);
    }

    private ProgressEvent<ResourceModel, CallbackContext> modifyParameters(final ProgressEvent<ResourceModel, CallbackContext> progress,
                                                                           final Map<String, Parameter> currentDBParameters,
                                                                           final AmazonWebServicesClientProxy proxy,
                                                                           final ProxyClient<RdsClient> proxyClient,
                                                                           final RequestLogger requestLogger) {
        ResourceModel model = progress.getResourceModel();
        CallbackContext callbackContext = progress.getCallbackContext();
        Map<String, Parameter> parametersToModify = getModifiableParameters(model, currentDBParameters);
        for (List<Parameter> paramsPartition : Iterables.partition(parametersToModify.values(), MAX_PARAMETERS_PER_REQUEST)) {  //modify api call is limited to 20 parameter per request
            ProgressEvent<ResourceModel, CallbackContext> progressEvent = modifyParameters(proxyClient, proxy, callbackContext, paramsPartition, model, requestLogger);
            if (progressEvent.isFailed()) return progressEvent;
        }
        requestLogger.log("ModifiedParameter", parametersToModify);
        return ProgressEvent.progress(model, callbackContext);

    }

    private ProgressEvent<ResourceModel, CallbackContext> modifyParameters(final ProxyClient<RdsClient> proxyClient,
                                                                           final AmazonWebServicesClientProxy proxy,
                                                                           final CallbackContext callbackContext,
                                                                           final List<Parameter> paramsPartition,
                                                                           final ResourceModel model,
                                                                           final RequestLogger requestLogger) {
        return proxy.initiate("rds::modify-db-parameter-group", proxyClient, model, callbackContext)
                .translateToServiceRequest((resourceModel) -> Translator.modifyDbParameterGroupRequest(resourceModel, paramsPartition))
                .makeServiceCall(requestLogger.log((request, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(request, proxyInvocation.client()::modifyDBParameterGroup)))
                .handleError((describeDbParameterGroupsRequest, exception, client, resourceModel, ctx) ->
                        Commons.handleException(
                                ProgressEvent.progress(resourceModel, ctx),
                                exception,
                                DEFAULT_DB_PARAMETER_GROUP_ERROR_RULE_SET
                        ))
                .progress();
    }

    private ProgressEvent<ResourceModel, CallbackContext> resetParameters(final AmazonWebServicesClientProxy proxy,
                                                                          final ResourceModel model,
                                                                          final CallbackContext callbackContext,
                                                                          final List<Parameter> paramsPartition,
                                                                          final ProxyClient<RdsClient> proxyClient,
                                                                          final RequestLogger requestLogger) {
        return proxy.initiate("rds::reset-db-parameter-group", proxyClient, model, callbackContext)
                .translateToServiceRequest((resourceModel) -> Translator.resetDbParametersRequest(resourceModel, paramsPartition))
                .makeServiceCall(requestLogger.log((request, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(request, proxyInvocation.client()::resetDBParameterGroup)))
                .handleError((describeDbParameterGroupsRequest, exception, client, resourceModel, ctx) ->
                        Commons.handleException(
                                ProgressEvent.progress(resourceModel, ctx),
                                exception,
                                DEFAULT_DB_PARAMETER_GROUP_ERROR_RULE_SET
                        ))
                .progress();
    }

    private Map<String, Parameter> getModifiableParameters(final ResourceModel model,
                                                           final Map<String, Parameter> currentDBParameters) {
        Map<String, Parameter> parametersToModify = Maps.newHashMap(currentDBParameters);
        Map<String, Object> modelParameters = Optional.ofNullable(model.getParameters()).orElse(Collections.emptyMap());
        parametersToModify.keySet().retainAll(modelParameters.keySet());
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

    private ProgressEvent<ResourceModel, CallbackContext> validateModelParameters(final ProgressEvent<ResourceModel, CallbackContext> progress,
                                                                                  final Map<String, Parameter> defaultEngineParameters,
                                                                                  final RequestLogger requestLogger) {
        Map<String, Object> modelParameters = Optional.ofNullable(progress.getResourceModel().getParameters()).orElse(Collections.emptyMap());
        Set<String> invalidParameters = modelParameters.entrySet().stream()
                .filter(entry -> {
                    String parameterName = entry.getKey();
                    String newParameterValue = String.valueOf(entry.getValue());
                    Parameter defaultParameter = defaultEngineParameters.get(parameterName);
                    if (!defaultEngineParameters.containsKey(parameterName)) return true;
                    //Parameter is not modifiable and input model contains different value from default value
                    return !defaultParameter.isModifiable() && !defaultParameter.parameterValue().equals(newParameterValue);
                })
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());

        if (!invalidParameters.isEmpty()) {
            requestLogger.log("InvalidParameters", invalidParameters);
            return ProgressEvent.defaultFailureHandler(
                    new CfnInvalidRequestException("Invalid / unmodifiable / Unsupported DB Parameter: " + invalidParameters.stream().findFirst().get()),
                    HandlerErrorCode.InvalidRequest
            );
        }
        return ProgressEvent.progress(progress.getResourceModel(), progress.getCallbackContext());
    }

    private Map<String, Parameter> getParametersToReset(final ResourceModel model,
                                                        final Map<String, Parameter> defaultEngineParameters,
                                                        final Map<String, Parameter> currentParameters) {
        return currentParameters.entrySet()
                .stream()
                .filter(entry -> {
                    String parameterName = entry.getKey();
                    String currentParameterValue = entry.getValue().parameterValue();
                    String defaultParameterValue = defaultEngineParameters.get(parameterName).parameterValue();
                    Map<String, Object> parametersToModify = model.getParameters();
                    return parametersToModify != null && currentParameterValue != null
                            && !currentParameterValue.equals(defaultParameterValue)
                            && !parametersToModify.containsKey(parameterName);
                })
                .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue()));
    }

    private ProgressEvent<ResourceModel, CallbackContext> describeCurrentDBParameters(final ProgressEvent<ResourceModel, CallbackContext> progress,
                                                                                      final Map<String, Parameter> currentDBParameters,
                                                                                      final AmazonWebServicesClientProxy proxy,
                                                                                      final ProxyClient<RdsClient> proxyClient,
                                                                                      final RequestLogger requestLogger) {
        return proxy.initiate("rds::describe-db-parameters", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest((resourceModel) -> Translator.describeDbParametersRequest(resourceModel))
                .makeServiceCall(requestLogger.log((request, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeIterableV2(request, proxyInvocation.client()::describeDBParametersPaginator)))
                .handleError((describeDBParametersPaginatorRequest, exception, client, resourceModel, ctx) ->
                        Commons.handleException(
                                ProgressEvent.progress(resourceModel, ctx),
                                exception,
                                DEFAULT_DB_PARAMETER_GROUP_ERROR_RULE_SET
                        ))
                .done((describeDbParameterGroupsRequest, describeDbParameterGroupsResponse, proxyInvocation, resourceModel, context) -> {
                    currentDBParameters.putAll(
                            describeDbParameterGroupsResponse.stream()
                                    .flatMap(describeDbParametersResponse -> describeDbParametersResponse.parameters().stream())
                                    .collect(Collectors.toMap(Parameter::parameterName, Function.identity()))
                    );
                    return ProgressEvent.progress(resourceModel, context);
                });
    }

    private ProgressEvent<ResourceModel, CallbackContext> describeDefaultEngineParameters(final ProgressEvent<ResourceModel, CallbackContext> progress,
                                                                                          final Map<String, Parameter> defaultEngineParameters,
                                                                                          final AmazonWebServicesClientProxy proxy,
                                                                                          final ProxyClient<RdsClient> proxyClient,
                                                                                          final RequestLogger requestLogger) {
        return proxy.initiate("rds::default-engine-db-parameters", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest((resourceModel) -> Translator.describeEngineDefaultParametersRequest(resourceModel))
                .makeServiceCall(requestLogger.log((request, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeIterableV2(request, proxyInvocation.client()::describeEngineDefaultParametersPaginator)))
                .handleError((describeEngineDefaultParametersPaginatorRequest, exception, client, resourceModel, ctx) ->
                        Commons.handleException(
                                ProgressEvent.progress(resourceModel, ctx),
                                exception,
                                DEFAULT_DB_PARAMETER_GROUP_ERROR_RULE_SET
                        ))
                .done((describeEngineDefaultParametersRequest, describeEngineDefaultParametersResponse, proxyInvocation, resourceModel, context) -> {
                    defaultEngineParameters.putAll(
                            describeEngineDefaultParametersResponse.stream()
                                    .flatMap(describeDbParametersResponse -> describeDbParametersResponse.engineDefaults().parameters().stream())
                                    .collect(Collectors.toMap(Parameter::parameterName, Function.identity()))
                    );
                    return ProgressEvent.progress(resourceModel, context);
                });

    }

    protected abstract ProgressEvent<ResourceModel, CallbackContext> handleRequest(final AmazonWebServicesClientProxy proxy,
                                                                                   final ResourceHandlerRequest<ResourceModel> request,
                                                                                   final CallbackContext callbackContext,
                                                                                   final ProxyClient<RdsClient> proxyClient,
                                                                                   final RequestLogger requestLogger);
}
