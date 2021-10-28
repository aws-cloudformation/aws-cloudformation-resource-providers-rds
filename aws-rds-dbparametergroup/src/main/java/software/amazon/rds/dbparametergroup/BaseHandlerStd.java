package software.amazon.rds.dbparametergroup;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import software.amazon.awssdk.core.exception.RetryableException;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DbParameterGroupAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.DbParameterGroupNotFoundException;
import software.amazon.awssdk.services.rds.model.DbParameterGroupQuotaExceededException;
import software.amazon.awssdk.services.rds.model.InvalidDbParameterGroupStateException;
import software.amazon.awssdk.services.rds.model.Parameter;
import software.amazon.awssdk.services.rds.model.RdsRequest;
import software.amazon.cloudformation.exceptions.CfnAccessDeniedException;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.delay.Constant;

public abstract class BaseHandlerStd extends BaseHandler<CallbackContext> {
    protected static int MAX_LENGTH_GROUP_NAME = 255;
    protected static int NO_CALLBACK_DELAY = 0;
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

    protected ProgressEvent<ResourceModel, CallbackContext> softFailAccessDenied(final Supplier<ProgressEvent<ResourceModel, CallbackContext>> eventSupplier,
                                                                                 final ResourceModel model,
                                                                                 final CallbackContext callbackContext) {
        try {
            return eventSupplier.get();
        } catch (final CfnAccessDeniedException e) {
            return ProgressEvent.progress(model, callbackContext);
        }
    }

    public ProgressEvent<ResourceModel, CallbackContext> handleException(final Logger logger,
                                                                         final RdsRequest rdsRequest,
                                                                         final Exception e) {
        logger.log(String.format("Exception:\n%s\nWhile processing request:\n%s", e.getMessage(), rdsRequest.toString()));
        if (
                e instanceof DbParameterGroupAlreadyExistsException
        ) {
            return ProgressEvent.defaultFailureHandler(e, HandlerErrorCode.AlreadyExists);
        } else if (
                e instanceof DbParameterGroupQuotaExceededException
        ) {
            return ProgressEvent.defaultFailureHandler(e, HandlerErrorCode.ServiceLimitExceeded);
        } else if (
                e instanceof DbParameterGroupNotFoundException
        ) {
            return ProgressEvent.defaultFailureHandler(e, HandlerErrorCode.NotFound);
        } else if (
                e instanceof InvalidDbParameterGroupStateException
        ) {
            throw RetryableException.builder().cause(e).build(); // current behaviour is retry on InvalidDbParameterGroupState exception
        } else if (
                e.getMessage().equals("Rate exceeded")
        ) {
            return ProgressEvent.defaultFailureHandler(e, HandlerErrorCode.Throttling); //Request throttled from rds service
        }
        throw new CfnGeneralServiceException(e);
    }

    protected ProgressEvent<ResourceModel, CallbackContext> applyParameters(final Logger logger,
                                                                            final AmazonWebServicesClientProxy proxy,
                                                                            final ProxyClient<RdsClient> proxyClient,
                                                                            final ResourceModel model,
                                                                            final CallbackContext callbackContext) {
        //isParametersApplied flag for unit testing
        if (callbackContext.isParametersApplied())
            return ProgressEvent.defaultInProgressHandler(callbackContext, NO_CALLBACK_DELAY, model);
        callbackContext.setParametersApplied(true);

        Map<String, Parameter> defaultEngineParameters = Maps.newHashMap();
        Map<String, Parameter> currentDBParameters = Maps.newHashMap();

        return ProgressEvent.progress(model, callbackContext)
            .then(progressEvent -> describeDefaultEngineParametersCall(logger, progressEvent, defaultEngineParameters, proxy, proxyClient))
            .then(progressEvent -> validateModelParameters(logger, progressEvent, defaultEngineParameters))
            .then(progressEvent -> describeCurrentDBParametersCall(logger, progressEvent, currentDBParameters, proxy, proxyClient))
            .then(progressEvent -> resetParameters(logger, progressEvent, defaultEngineParameters, currentDBParameters, proxy,proxyClient))
            .then(progressEvent -> modifyParameters(logger, progressEvent, currentDBParameters, proxy, proxyClient));
    }

    private ProgressEvent<ResourceModel, CallbackContext> resetParameters(final Logger logger,
                                                                          final ProgressEvent<ResourceModel, CallbackContext> progress,
                                                                          final Map<String, Parameter> defaultEngineParameters,
                                                                          final Map<String, Parameter> currentDBParameters,
                                                                          final AmazonWebServicesClientProxy proxy,
                                                                          final ProxyClient<RdsClient> proxyClient) {
        ResourceModel model = progress.getResourceModel();
        CallbackContext callbackContext = progress.getCallbackContext();
        Map<String, Parameter> parametersToReset = getParametersToReset(model, defaultEngineParameters, currentDBParameters);
        for (List<Parameter> paramsPartition : Iterables.partition(parametersToReset.values(), MAX_PARAMETERS_PER_REQUEST)) {  //modify api call is limited to 20 parameter per request
            ProgressEvent<ResourceModel, CallbackContext> progressEvent = makeResetParametersCall(logger, proxy, model, callbackContext, paramsPartition, proxyClient);
            if (progressEvent.isFailed()) return progressEvent;
        }
        return ProgressEvent.progress(model, callbackContext);
    }

    private ProgressEvent<ResourceModel, CallbackContext> modifyParameters(final Logger logger,
                                                                           final ProgressEvent<ResourceModel, CallbackContext> progress,
                                                                           final Map<String, Parameter> currentDBParameters,
                                                                           final AmazonWebServicesClientProxy proxy,
                                                                           final ProxyClient<RdsClient> proxyClient) {
        ResourceModel model = progress.getResourceModel();
        CallbackContext callbackContext = progress.getCallbackContext();
        Map<String, Parameter> parametersToModify = getModifiableParameters(model, currentDBParameters);
        for (List<Parameter> paramsPartition : Iterables.partition(parametersToModify.values(), MAX_PARAMETERS_PER_REQUEST)) {  //modify api call is limited to 20 parameter per request
            ProgressEvent<ResourceModel, CallbackContext> progressEvent = makeModifyParametersCall(logger, proxyClient, proxy, callbackContext, paramsPartition, model);
            if (progressEvent.isFailed()) return progressEvent;
        }
        return ProgressEvent.progress(model, callbackContext);

    }

    private ProgressEvent<ResourceModel, CallbackContext> makeModifyParametersCall(final Logger logger,
                                                                                   final ProxyClient<RdsClient> proxyClient,
                                                                                   final AmazonWebServicesClientProxy proxy,
                                                                                   final CallbackContext callbackContext,
                                                                                   final List<Parameter> paramsPartition,
                                                                                   final ResourceModel model) {
        logger.log(String.format("Modifying %d parameters...", paramsPartition.size()));
        return proxy.initiate("rds::modify-db-parameter-group", proxyClient, model, callbackContext)
            .translateToServiceRequest((resourceModel) -> Translator.modifyDbParameterGroupRequest(resourceModel, paramsPartition))
            .makeServiceCall((request, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(request, proxyInvocation.client()::modifyDBParameterGroup))
            .handleError((describeDbParameterGroupsRequest, exception, client, resourceModel, ctx) -> handleException(logger, describeDbParameterGroupsRequest, exception))
            .progress();
    }

    private ProgressEvent<ResourceModel, CallbackContext> makeResetParametersCall(final Logger logger,
                                                                                  final AmazonWebServicesClientProxy proxy,
                                                                                  final ResourceModel model,
                                                                                  final CallbackContext callbackContext,
                                                                                  final List<Parameter> paramsPartition,
                                                                                  final ProxyClient<RdsClient> proxyClient) {
        logger.log(String.format("Resetting %d parameters...", paramsPartition.size()));
        return proxy.initiate("rds::reset-db-parameter-group", proxyClient, model, callbackContext)
            .translateToServiceRequest((resourceModel) -> Translator.resetDbParametersRequest(resourceModel, paramsPartition))
            .makeServiceCall((request, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(request, proxyInvocation.client()::resetDBParameterGroup))
            .handleError((describeDbParameterGroupsRequest, exception, client, resourceModel, ctx) -> handleException(logger, describeDbParameterGroupsRequest, exception))
            .progress();
    }

    private Map<String, Parameter> getModifiableParameters(final ResourceModel model,
                                                           final Map<String, Parameter> currentDBParameters) {
        Map<String, Parameter> parametersToModify = Maps.newHashMap(currentDBParameters);
        parametersToModify.keySet().retainAll(model.getParameters().keySet());
        return parametersToModify.entrySet()
            .stream()
            //filter to parameters want to modify and its value is different from already exist value
            .filter(entry -> {
                String parameterName = entry.getKey();
                String currentParameterValue = entry.getValue().parameterValue();
                String newParameterValue = String.valueOf(model.getParameters().get(parameterName));
                return !newParameterValue.equals(currentParameterValue);
            })
            .collect(Collectors.toMap(Map.Entry::getKey,
                entry -> {
                    String parameterName = entry.getKey();
                    String newValue = String.valueOf(model.getParameters().get(parameterName));
                    Parameter defaultParameter = entry.getValue();
                    return Translator.buildParameterWithNewValue(newValue, defaultParameter);
                })
            );
    }

    private ProgressEvent<ResourceModel, CallbackContext> validateModelParameters(final Logger logger,
                                                                                  final ProgressEvent<ResourceModel, CallbackContext> progress,
                                                                                  final Map<String, Parameter> defaultEngineParameters) {
        Set<String> invalidParameters = progress.getResourceModel().getParameters().entrySet().stream()
            .filter(entry -> {
                String parameterName = entry.getKey();
                String newParameterValue = String.valueOf(entry.getValue());
                Parameter defaultParameter = defaultEngineParameters.get(parameterName);
                if (!defaultEngineParameters.containsKey(parameterName)) return true;
                //Parameter is not modifiable and input model contains different value from default value
                if (!defaultParameter.isModifiable() && !defaultParameter.parameterValue().equals(newParameterValue)) return true;
                return false;
            })
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());

        if (!invalidParameters.isEmpty()){
            logger.log(String.format(
                    "Invalid parameters: %s\nDefault engine parameters: %s",
                    invalidParameters.stream().collect(Collectors.joining(" , ")),
                    defaultEngineParameters.entrySet().stream()
                            .map(entry -> String.format("%s -> %s",entry.getKey(), entry.getValue().isModifiable()))
                            .collect(Collectors.joining(" , "))));
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

    private ProgressEvent<ResourceModel, CallbackContext> describeCurrentDBParametersCall(final Logger logger,
                                                                                          final ProgressEvent<ResourceModel, CallbackContext> progress,
                                                                                          final Map<String, Parameter> currentDBParameters,
                                                                                          final AmazonWebServicesClientProxy proxy,
                                                                                          ProxyClient<RdsClient> proxyClient) {
        return proxy.initiate("rds::describe-db-parameters", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
            .translateToServiceRequest((resourceModel) -> Translator.describeDbParametersRequest(resourceModel))
            .makeServiceCall((request, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeIterableV2(request, proxyInvocation.client()::describeDBParametersPaginator))
            .handleError((describeDBParametersPaginatorRequest, exception, client, resourceModel, ctx) -> handleException(logger, describeDBParametersPaginatorRequest, exception))
            .done((describeDbParameterGroupsRequest, describeDbParameterGroupsResponse, proxyInvocation, resourceModel, context) -> {
                currentDBParameters.putAll(
                    describeDbParameterGroupsResponse.stream()
                            .flatMap(describeDbParametersResponse -> describeDbParametersResponse.parameters().stream())
                            .collect(Collectors.toMap(Parameter::parameterName, Function.identity()))
                );
                return ProgressEvent.progress(resourceModel, context);
            });
    }

    private ProgressEvent<ResourceModel, CallbackContext> describeDefaultEngineParametersCall(final Logger logger,
                                                                                              final ProgressEvent<ResourceModel, CallbackContext> progress,
                                                                                              final Map<String, Parameter> defaultEngineParameters,
                                                                                              final AmazonWebServicesClientProxy proxy,
                                                                                              final ProxyClient<RdsClient> proxyClient) {
        return proxy.initiate("rds::default-engine-db-parameters", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
            .translateToServiceRequest((resourceModel) -> Translator.describeEngineDefaultParametersRequest(resourceModel))
            .makeServiceCall((request, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeIterableV2(request, proxyInvocation.client()::describeEngineDefaultParametersPaginator))
            .handleError((describeEngineDefaultParametersPaginatorRequest, exception, client, resourceModel, ctx) -> handleException(logger, describeEngineDefaultParametersPaginatorRequest, exception))
            .done((describeEngineDefaultParametersRequest, describeEngineDefaultParametersResponse, proxyInvocation, resourceModel, context) -> {
                defaultEngineParameters.putAll(
                    describeEngineDefaultParametersResponse.stream()
                            .flatMap(describeDbParametersResponse -> describeDbParametersResponse.engineDefaults().parameters().stream())
                            .collect(Collectors.toMap(Parameter::parameterName, Function.identity()))
                );
                return ProgressEvent.progress(resourceModel, context);
            });

    }

    protected <K, V> Map<K, V> mergeMaps(final Map<K, V> m1,
                                         final Map<K, V> m2) {
        final Map<K, V> result = new HashMap<>();
        result.putAll(Optional.ofNullable(m1).orElse(Collections.emptyMap()));
        result.putAll(Optional.ofNullable(m2).orElse(Collections.emptyMap()));
        return result;
    }

    protected abstract ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger);
}
