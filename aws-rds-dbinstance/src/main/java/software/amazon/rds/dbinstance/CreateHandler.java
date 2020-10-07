package software.amazon.rds.dbinstance;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import com.amazonaws.util.CollectionUtils;
import com.amazonaws.util.StringUtils;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBInstance;
import software.amazon.awssdk.services.rds.model.DbInstanceNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.resource.IdentifierUtils;

public class CreateHandler extends BaseHandlerStd {

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProxyClient<Ec2Client> ec2ProxyClient,
            final Logger logger
    ) {
        final Collection<DBInstanceRole> desiredRoles = request.getDesiredResourceState().getAssociatedRoles();

        return ProgressEvent.progress(request.getDesiredResourceState(), callbackContext)
                .then(progress -> {
                    final ResourceModel model = request.getDesiredResourceState();
                    if (StringUtils.isNullOrEmpty(model.getDBInstanceIdentifier())) {
                        model.setDBInstanceIdentifier(IdentifierUtils.generateResourceIdentifier(
                                Optional.ofNullable(request.getStackId()).orElse(STACK_NAME),
                                Optional.ofNullable(request.getLogicalResourceIdentifier()).orElse(RESOURCE_IDENTIFIER),
                                request.getClientRequestToken(),
                                RESOURCE_ID_MAX_LENGTH
                        ).toLowerCase());
                    }
                    final Map<String, String> tags = mergeMaps(Arrays.asList(
                            request.getSystemTags(),
                            request.getDesiredResourceTags()
                    ));
                    model.setTags(Translator.translateTagsFromRequest(tags));
                    return ProgressEvent.progress(model, progress.getCallbackContext());
                })
                .then(progress -> {
                    if (isReadReplica(progress.getResourceModel())) {
                        return initiateCreateDbInstanceReadReplicaRequest(proxy, rdsProxyClient, progress);
                    } else if (isRestoreFromSnapshot(progress.getResourceModel())) {
                        return initiateRestoreDbInstanceFromSnapshotRequest(proxy, rdsProxyClient, progress);
                    }
                    return initiateCreateDbInstanceRequest(proxy, rdsProxyClient, progress);
                })
                .then(progress -> ensureEngineSet(proxy, rdsProxyClient, progress))
                .then(progress -> {
                    if (shouldUpdateAfterCreate(progress.getResourceModel())) {
                        return execOnce(progress, () -> {
                            return updateAfterCreate(proxy, rdsProxyClient, progress, request.getDesiredResourceState())
                                    .then(p -> {
                                        if (shouldReboot(p.getResourceModel())) {
                                            return rebootAwait(proxy, rdsProxyClient, p);
                                        }
                                        return p;
                                    });
                        }, CallbackContext::isUpdatedAfterCreate, CallbackContext::setUpdatedAfterCreate);
                    }
                    return progress;
                })
                .then(progress -> execOnce(progress, () -> {
                    return updateAssociatedRoles(proxy, rdsProxyClient, progress, Collections.emptyList(), desiredRoles);
                }, CallbackContext::isRolesUpdated, CallbackContext::setRolesUpdated))
                .then(progress -> new ReadHandler().handleRequest(proxy, request, progress.getCallbackContext(), rdsProxyClient, ec2ProxyClient, logger));
    }

    private ProgressEvent<ResourceModel, CallbackContext> initiateCreateDbInstanceRequest(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress
    ) {
        return proxy.initiate(
                "rds::create-db-instance",
                rdsProxyClient,
                progress.getResourceModel(),
                progress.getCallbackContext()
        ).translateToServiceRequest(Translator::createDbInstanceRequest)
                .backoffDelay(BACK_OFF)
                .makeServiceCall((createRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        createRequest,
                        proxyInvocation.client()::createDBInstance
                ))
                .stabilize((request, response, proxyInvocation, model, callbackContext) -> isDbInstanceStabilized(
                        proxyInvocation, model
                ))
                .handleError((request, exception, client, model, context) -> handleException(
                        ProgressEvent.progress(model, context),
                        exception
                ))
                .progress();
    }

    private ProgressEvent<ResourceModel, CallbackContext> initiateRestoreDbInstanceFromSnapshotRequest(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress
    ) {
        return proxy.initiate(
                "rds::restore-db-instance-from-snapshot",
                rdsProxyClient,
                progress.getResourceModel(),
                progress.getCallbackContext()
        ).translateToServiceRequest(Translator::restoreDbInstanceFromSnapshotRequest)
                .backoffDelay(BACK_OFF)
                .makeServiceCall((restoreRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        restoreRequest,
                        proxyInvocation.client()::restoreDBInstanceFromDBSnapshot
                ))
                .stabilize((request, response, proxyInvocation, model, callbackContext) -> isDbInstanceStabilized(
                        proxyInvocation, model
                ))
                .handleError((request, exception, client, model, context) -> handleException(
                        ProgressEvent.progress(model, context),
                        exception
                ))
                .progress();
    }

    private ProgressEvent<ResourceModel, CallbackContext> initiateCreateDbInstanceReadReplicaRequest(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress
    ) {
        return proxy.initiate(
                "rds::create-db-instance-read-replica",
                rdsProxyClient,
                progress.getResourceModel(),
                progress.getCallbackContext()
        ).translateToServiceRequest(Translator::createDbInstanceReadReplicaRequest)
                .backoffDelay(BACK_OFF)
                .makeServiceCall((createRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        createRequest,
                        proxyInvocation.client()::createDBInstanceReadReplica
                ))
                .stabilize((request, response, proxyInvocation, model, callbackContext) -> isDbInstanceStabilized(
                        proxyInvocation, model
                ))
                .handleError((request, exception, client, model, context) -> handleException(
                        ProgressEvent.progress(model, context),
                        exception
                ))
                .progress();
    }

    private ProgressEvent<ResourceModel, CallbackContext> updateAfterCreate(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final ResourceModel desiredModel
    ) {
        final Optional<DBInstance> maybeDbInstance = fetchDBInstance(rdsProxyClient, desiredModel);
        if (!maybeDbInstance.isPresent()) {
            throw DbInstanceNotFoundException.builder().build();
        }
        final ResourceModel previousModel = Translator.translateDbInstanceFromSdk(maybeDbInstance.get());
        return proxy.initiate("rds::modify-after-create-db-instance", rdsProxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(resourceModel -> Translator.modifyDbInstanceRequest(previousModel, desiredModel, false))
                .backoffDelay(BACK_OFF)
                .makeServiceCall((modifyRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        modifyRequest,
                        proxyInvocation.client()::modifyDBInstance
                ))
                .stabilize((request, response, proxyInvocation, model, callbackContext) -> isDbInstanceStabilized(
                        proxyInvocation, model
                ))
                .progress();
    }

    private boolean shouldReboot(final ResourceModel model) {
        return StringUtils.hasValue(model.getDBParameterGroupName());
    }

    private boolean shouldUpdateAfterCreate(final ResourceModel model) {
        return isReadReplica(model) || isRestoreFromSnapshot(model) || StringUtils.hasValue(model.getCACertificateIdentifier());
    }
}
