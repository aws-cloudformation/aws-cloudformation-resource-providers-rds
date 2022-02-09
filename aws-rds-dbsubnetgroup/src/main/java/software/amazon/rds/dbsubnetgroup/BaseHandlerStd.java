package software.amazon.rds.dbsubnetgroup;

import java.time.Duration;
import java.util.Map;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DbSubnetGroupAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.DbSubnetGroupNotFoundException;
import software.amazon.awssdk.services.rds.model.DbSubnetGroupQuotaExceededException;
import software.amazon.awssdk.services.rds.model.InvalidDbSubnetGroupStateException;
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
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.common.logging.ProxyClientLogger;
import software.amazon.rds.common.logging.RequestLogger;
import software.amazon.rds.common.printer.FilteredJsonPrinter;

public abstract class BaseHandlerStd extends BaseHandler<CallbackContext> {
    protected static final int DB_SUBNET_GROUP_NAME_LENGTH = 255;
    protected static final String DB_SUBNET_GROUP_STATUS_COMPLETE = "Complete";
    protected static final Constant CONSTANT = Constant.of().timeout(Duration.ofMinutes(120L))
            .delay(Duration.ofSeconds(30L)).build();

    protected static final ErrorRuleSet DEFAULT_DB_SUBNET_GROUP_ERROR_RULE_SET = ErrorRuleSet.builder()
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.AlreadyExists),
                    DbSubnetGroupAlreadyExistsException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.NotFound),
                    DbSubnetGroupNotFoundException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.ServiceLimitExceeded),
                    DbSubnetGroupQuotaExceededException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.ResourceConflict),
                    InvalidDbSubnetGroupStateException.class)
            .build()
            .orElse(Commons.DEFAULT_ERROR_RULE_SET);

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
                requestLogger -> handleRequest(
                        proxy,
                        request,
                        callbackContext != null ? callbackContext : new CallbackContext(),
                        ProxyClientLogger.newProxy(requestLogger, proxy.newProxy(ClientBuilder::getClient)),
                        logger
                ));
    }

    protected abstract ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            AmazonWebServicesClientProxy proxy,
            ResourceHandlerRequest<ResourceModel> request,
            CallbackContext callbackContext,
            ProxyClient<RdsClient> proxyClient,
            Logger logger);

    protected boolean isStabilized(final ResourceModel model, final ProxyClient<RdsClient> proxyClient) {
        final String status = proxyClient.injectCredentialsAndInvokeV2(
                Translator.describeDbSubnetGroupsRequest(model),
                proxyClient.client()::describeDBSubnetGroups)
                .dbSubnetGroups().stream().findFirst().get().subnetGroupStatus();
        return status.equals(DB_SUBNET_GROUP_STATUS_COMPLETE);
    }

    protected boolean isDeleted(final ResourceModel model,
                                final ProxyClient<RdsClient> proxyClient) {
        try {
            proxyClient.injectCredentialsAndInvokeV2(
                    Translator.describeDbSubnetGroupsRequest(model),
                    proxyClient.client()::describeDBSubnetGroups);
            return false;
        } catch (DbSubnetGroupNotFoundException e) {
            return true;
        }
    }

    protected ProgressEvent<ResourceModel, CallbackContext> tagResource(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Map<String, String> previousTags,
            final Map<String, String> desiredTags) {
        return proxy.initiate("rds::tag-dbsubnet-group", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(Translator::describeDbSubnetGroupsRequest)
                .makeServiceCall((describeDbSubnetGroupsRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(describeDbSubnetGroupsRequest, proxyInvocation.client()::describeDBSubnetGroups))
                .done((describeDbSubnetGroupsRequest, describeDbSubnetGroupsResponse, proxyInvocation, resourceModel, context) -> {
                    final String arn = describeDbSubnetGroupsResponse.dbSubnetGroups().stream().findFirst().get().dbSubnetGroupArn();
                    return Tagging.updateTags(
                            proxyInvocation,
                            arn,
                            ProgressEvent.progress(resourceModel, context),
                            previousTags,
                            desiredTags,
                            DEFAULT_DB_SUBNET_GROUP_ERROR_RULE_SET
                    );
                });
    }
}
