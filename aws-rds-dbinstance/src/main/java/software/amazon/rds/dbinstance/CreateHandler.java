package software.amazon.rds.dbinstance;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

import org.apache.commons.lang3.BooleanUtils;

import com.amazonaws.util.CollectionUtils;
import com.amazonaws.util.StringUtils;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBSnapshot;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.HandlerMethod;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.common.util.IdentifierFactory;

public class CreateHandler extends BaseHandlerStd {

    public static final String ILLEGAL_DELETION_POLICY_ERR = "DeletionPolicy:Snapshot cannot be specified for a cluster instance, use deletion policy on the cluster instead.";

    private static final IdentifierFactory instanceIdentifierFactory = new IdentifierFactory(
            STACK_NAME,
            RESOURCE_IDENTIFIER,
            RESOURCE_ID_MAX_LENGTH
    );

    public CreateHandler() {
        this(DEFAULT_DB_INSTANCE_HANDLER_CONFIG);
    }

    public CreateHandler(final HandlerConfig config) {
        super(config);
    }

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProxyClient<Ec2Client> ec2ProxyClient,
            final Logger logger
    ) {
        final ResourceModel model = request.getDesiredResourceState();
        final Collection<DBInstanceRole> desiredRoles = model.getAssociatedRoles();

        if (BooleanUtils.isTrue(request.getSnapshotRequested()) && StringUtils.hasValue(model.getDBClusterIdentifier())) {
            return ProgressEvent.failed(model, callbackContext, HandlerErrorCode.InvalidRequest, ILLEGAL_DELETION_POLICY_ERR);
        }

        if (StringUtils.isNullOrEmpty(model.getDBInstanceIdentifier())) {
            model.setDBInstanceIdentifier(instanceIdentifierFactory.newIdentifier()
                    .withStackId(request.getStackId())
                    .withResourceId(request.getLogicalResourceIdentifier())
                    .withRequestToken(request.getClientRequestToken())
                    .toString());
        }

        final Tagging.TagSet allTags = Tagging.TagSet.builder()
                .systemTags(Tagging.translateTagsToSdk(request.getSystemTags()))
                .stackTags(Tagging.translateTagsToSdk(request.getDesiredResourceTags()))
                .resourceTags(Translator.translateTagsToSdk(request.getDesiredResourceState().getTags()))
                .build();

        return ProgressEvent.progress(model, callbackContext)
                .then(progress -> Commons.execOnce(progress, () -> {
                    if (isReadReplica(progress.getResourceModel())) {
                        return safeCreate(this::createDbInstanceReadReplica, proxy, rdsProxyClient, progress, allTags);
                    } else if (isRestoreFromSnapshot(progress.getResourceModel())) {
                        return safeCreate(this::restoreDbInstanceFromSnapshot, proxy, rdsProxyClient, progress, allTags);
                    }
                    return safeCreate(this::createDbInstance, proxy, rdsProxyClient, progress, allTags);
                }, CallbackContext::isCreated, CallbackContext::setCreated))
                .then(progress -> Commons.execOnce(progress, () -> {
                    final Tagging.TagSet extraTags = Tagging.TagSet.builder()
                            .stackTags(allTags.getStackTags())
                            .resourceTags(allTags.getResourceTags())
                            .build();
                    return updateTags(proxy, rdsProxyClient, progress, Tagging.TagSet.emptySet(), extraTags);
                }, CallbackContext::isCreateTagComplete, CallbackContext::setCreateTagComplete))
                .then(progress -> ensureEngineSet(rdsProxyClient, progress))
                .then(progress -> {
                    if (shouldUpdateAfterCreate(progress.getResourceModel())) {
                        return Commons.execOnce(progress, () ->
                                                updateDbInstanceAfterCreate(proxy, rdsProxyClient, progress, request.getDesiredResourceState()),
                                        CallbackContext::isUpdated, CallbackContext::setUpdated)
                                .then(p -> Commons.execOnce(p, () -> {
                                    if (shouldReboot(p.getResourceModel())) {
                                        return rebootAwait(proxy, rdsProxyClient, p);
                                    }
                                    return p;
                                }, CallbackContext::isRebooted, CallbackContext::setRebooted));
                    }
                    return progress;
                })
                .then(progress -> Commons.execOnce(progress, () ->
                                updateAssociatedRoles(proxy, rdsProxyClient, progress, Collections.emptyList(), desiredRoles),
                        CallbackContext::isUpdatedRoles, CallbackContext::setUpdatedRoles))
                .then(progress -> new ReadHandler().handleRequest(proxy, request, progress.getCallbackContext(), rdsProxyClient, ec2ProxyClient, logger));
    }

    private ProgressEvent<ResourceModel, CallbackContext> safeCreate(
            final HandlerMethod<ResourceModel, CallbackContext> createMethod,
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Tagging.TagSet allTags
    ) {
        final ProgressEvent<ResourceModel, CallbackContext> result = createMethod.invoke(proxy, rdsProxyClient, progress, allTags);
        if (HandlerErrorCode.AccessDenied.equals(result.getErrorCode())) {
            final Tagging.TagSet systemTags = Tagging.TagSet.builder().systemTags(allTags.getSystemTags()).build();
            return createMethod.invoke(proxy, rdsProxyClient, progress, systemTags);
        }
        result.getCallbackContext().setCreateTagComplete(true);
        return result;
    }

    private ProgressEvent<ResourceModel, CallbackContext> createDbInstance(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Tagging.TagSet tagSet
    ) {
        return proxy.initiate(
                        "rds::create-db-instance",
                        rdsProxyClient,
                        progress.getResourceModel(),
                        progress.getCallbackContext()
                ).translateToServiceRequest(model -> Translator.createDbInstanceRequest(model, tagSet))
                .backoffDelay(config.getBackoff())
                .makeServiceCall((createRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        createRequest,
                        proxyInvocation.client()::createDBInstance
                ))
                .stabilize((request, response, proxyInvocation, model, context) ->
                        isDbInstanceStabilized(proxyInvocation, model))
                .handleError((request, exception, client, model, context) -> Commons.handleException(
                        ProgressEvent.progress(model, context),
                        exception,
                        CREATE_DB_INSTANCE_ERROR_RULE_SET
                ))
                .progress();
    }

    private ProgressEvent<ResourceModel, CallbackContext> restoreDbInstanceFromSnapshot(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Tagging.TagSet tagSet
    ) {
        final ResourceModel resourceModel = progress.getResourceModel();
        if (resourceModel.getMultiAZ() == null) {
            try {
                final DBSnapshot snapshot = fetchDBSnapshot(rdsProxyClient, resourceModel);
                final String engine = snapshot.engine();
                resourceModel.setMultiAZ(getDefaultMultiAzForEngine(engine));
            } catch (Exception e) {
                return Commons.handleException(progress, e, RESTORE_DB_INSTANCE_ERROR_RULE_SET);
            }
        }

        return proxy.initiate(
                        "rds::restore-db-instance-from-snapshot",
                        rdsProxyClient,
                        resourceModel,
                        progress.getCallbackContext()
                ).translateToServiceRequest(model -> Translator.restoreDbInstanceFromSnapshotRequest(model, tagSet))
                .backoffDelay(config.getBackoff())
                .makeServiceCall((restoreRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        restoreRequest,
                        proxyInvocation.client()::restoreDBInstanceFromDBSnapshot
                ))
                .stabilize((request, response, proxyInvocation, model, context) ->
                        isDbInstanceStabilized(proxyInvocation, model))
                .handleError((request, exception, client, model, context) -> Commons.handleException(
                        ProgressEvent.progress(model, context),
                        exception,
                        RESTORE_DB_INSTANCE_ERROR_RULE_SET
                ))
                .progress();
    }

    private ProgressEvent<ResourceModel, CallbackContext> createDbInstanceReadReplica(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Tagging.TagSet tagSet
    ) {
        return proxy.initiate(
                        "rds::create-db-instance-read-replica",
                        rdsProxyClient,
                        progress.getResourceModel(),
                        progress.getCallbackContext()
                ).translateToServiceRequest(model -> Translator.createDbInstanceReadReplicaRequest(model, tagSet))
                .backoffDelay(config.getBackoff())
                .makeServiceCall((createRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        createRequest,
                        proxyInvocation.client()::createDBInstanceReadReplica
                ))
                .stabilize((request, response, proxyInvocation, model, context) ->
                        isDbInstanceStabilized(proxyInvocation, model))
                .handleError((request, exception, client, model, context) -> Commons.handleException(
                        ProgressEvent.progress(model, context),
                        exception,
                        CREATE_DB_INSTANCE_READ_REPLICA_ERROR_RULE_SET
                ))
                .progress();
    }

    private ProgressEvent<ResourceModel, CallbackContext> updateDbInstanceAfterCreate(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final ResourceModel desiredModel
    ) {
        return proxy.initiate(
                        "rds::modify-after-create-db-instance",
                        rdsProxyClient,
                        progress.getResourceModel(),
                        progress.getCallbackContext()
                )
                .translateToServiceRequest(resourceModel -> Translator.modifyDbInstanceRequest(null, desiredModel, false))
                .backoffDelay(config.getBackoff())
                .makeServiceCall((modifyRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        modifyRequest,
                        proxyInvocation.client()::modifyDBInstance
                ))
                .stabilize((request, response, proxyInvocation, model, context) -> withProbing(
                        context,
                        "modify-after-create-db-instance-available",
                        3,
                        () -> isDbInstanceStabilized(proxyInvocation, model)
                ))
                .handleError((request, exception, client, model, context) -> Commons.handleException(
                        ProgressEvent.progress(model, context),
                        exception,
                        MODIFY_DB_INSTANCE_ERROR_RULE_SET
                ))
                .progress();
    }

    private boolean shouldReboot(final ResourceModel model) {
        return StringUtils.hasValue(model.getDBParameterGroupName());
    }

    private boolean shouldUpdateAfterCreate(final ResourceModel model) {
        return (isReadReplica(model) || isRestoreFromSnapshot(model) || isCertificateAuthorityApplied(model)) &&
                (
                        !CollectionUtils.isNullOrEmpty(model.getDBSecurityGroups()) ||
                                StringUtils.hasValue(model.getAllocatedStorage()) ||
                                StringUtils.hasValue(model.getCACertificateIdentifier()) ||
                                StringUtils.hasValue(model.getDBParameterGroupName()) ||
                                StringUtils.hasValue(model.getEngineVersion()) ||
                                StringUtils.hasValue(model.getMasterUserPassword()) ||
                                StringUtils.hasValue(model.getPreferredBackupWindow()) ||
                                StringUtils.hasValue(model.getPreferredMaintenanceWindow()) ||
                                Optional.ofNullable(model.getBackupRetentionPeriod()).orElse(0) > 0 ||
                                Optional.ofNullable(model.getIops()).orElse(0) > 0 ||
                                Optional.ofNullable(model.getMaxAllocatedStorage()).orElse(0) > 0
                );
    }

    private boolean isCertificateAuthorityApplied(final ResourceModel model) {
        return StringUtils.hasValue(model.getCACertificateIdentifier());
    }

    private Boolean getDefaultMultiAzForEngine(final String engine) {
        if (SQLSERVER_ENGINES_WITH_MIRRORING.contains(engine)) {
            return null;
        }
        return false;
    }
}
