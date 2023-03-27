package software.amazon.rds.optiongroup;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.rds.model.OptionGroupAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.OptionGroupNotFoundException;
import software.amazon.awssdk.services.rds.model.OptionGroupQuotaExceededException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
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

public abstract class BaseHandlerStd extends BaseHandler<CallbackContext> {

    protected static final String STACK_NAME = "rds";
    protected static final String RESOURCE_IDENTIFIER = "optiongroup";
    protected static final int RESOURCE_ID_MAX_LENGTH = 255;

    protected static final Constant BACKOFF_DELAY = Constant.of()
            .timeout(Duration.ofSeconds(150L))
            .delay(Duration.ofSeconds(5L))
            .build();

    protected HandlerConfig config;

    public BaseHandlerStd(final HandlerConfig config) {
        super();
        this.config = config;
    }

    protected static final ErrorRuleSet DEFAULT_OPTION_GROUP_ERROR_RULE_SET = ErrorRuleSet
            .extend(Commons.DEFAULT_ERROR_RULE_SET)
            .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.ResourceConflict),
                    ErrorCode.InvalidOptionGroupStateFault)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.AlreadyExists),
                    OptionGroupAlreadyExistsException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.NotFound),
                    OptionGroupNotFoundException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.ServiceLimitExceeded),
                    OptionGroupQuotaExceededException.class)
            .build();

    private final FilteredJsonPrinter PARAMETERS_FILTER = new FilteredJsonPrinter();

    @Override
    public final ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final Logger logger) {
        return RequestLogger.handleRequest(
                logger,
                request,
                PARAMETERS_FILTER,
                requestLogger -> handleRequest(
                        proxy,
                        request,
                        callbackContext != null ? callbackContext : new CallbackContext(),
                        new LoggingProxyClient<>(requestLogger, proxy.newProxy(new ClientBuilder()::getClient)),
                        logger
                ));
    }

    protected abstract ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger);

    protected ProgressEvent<ResourceModel, CallbackContext> updateOptionGroupConfigurations(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress) {
        return proxy.initiate("rds::update-option-group", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(Translator::modifyOptionGroupRequest)
                .makeServiceCall((modifyRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        modifyRequest,
                        proxyInvocation.client()::modifyOptionGroup
                ))
                .handleError((describeRequest, exception, client, resourceModel, ctx) -> Commons.handleException(
                        ProgressEvent.progress(resourceModel, ctx),
                        exception,
                        DEFAULT_OPTION_GROUP_ERROR_RULE_SET
                ))
                .progress();
    }

    protected ProgressEvent<ResourceModel, CallbackContext> updateTags(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Tagging.TagSet previousTags,
            final Tagging.TagSet desiredTags
    ) {
        return proxy.initiate("rds::tag-option-group", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(Translator::describeOptionGroupsRequest)
                .makeServiceCall((describeRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        describeRequest,
                        proxyInvocation.client()::describeOptionGroups
                )).handleError((describeRequest, exception, client, resourceModel, ctx) -> Commons.handleException(
                        ProgressEvent.progress(resourceModel, ctx),
                        exception,
                        DEFAULT_OPTION_GROUP_ERROR_RULE_SET
                ))
                .done((describeRequest, describeResponse, invocation, resourceModel, ctx) -> {
                    final Tagging.TagSet tagsToAdd = Tagging.exclude(desiredTags, previousTags);
                    final Tagging.TagSet tagsToRemove = Tagging.exclude(previousTags, desiredTags);

                    if (tagsToRemove.isEmpty() && tagsToAdd.isEmpty()) {
                        return progress;
                    }

                    final String arn = describeResponse.optionGroupsList().stream().findFirst().get().optionGroupArn();
                    try {
                        Tagging.removeTags(proxyClient, arn, Tagging.translateTagsToSdk(tagsToRemove));
                        Tagging.addTags(proxyClient, arn, Tagging.translateTagsToSdk(tagsToAdd));
                    } catch (Exception exception) {
                        return Commons.handleException(
                                progress,
                                exception,
                                DEFAULT_OPTION_GROUP_ERROR_RULE_SET.extendWith(
                                        Tagging.bestEffortErrorRuleSet(
                                                tagsToAdd,
                                                tagsToRemove,
                                                Tagging.SOFT_FAIL_IN_PROGRESS_TAGGING_ERROR_RULE_SET,
                                                Tagging.HARD_FAIL_TAG_ERROR_RULE_SET
                                        )
                                )
                        );
                    }
                    return ProgressEvent.progress(resourceModel, ctx);
                });
    }

    protected List<Tag> listTags(final ProxyClient<RdsClient> proxyClient, final String arn) {
        final ListTagsForResourceResponse listTagsForResourceResponse = proxyClient.injectCredentialsAndInvokeV2(
                Translator.listTagsForResourceRequest(arn),
                proxyClient.client()::listTagsForResource
        );
        return Translator.translateTagsFromSdk(listTagsForResourceResponse.tagList());
    }
}
