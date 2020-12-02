package software.amazon.rds.dbsecuritygroup;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.amazonaws.util.CollectionUtils;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBSecurityGroup;
import software.amazon.awssdk.services.rds.model.DbSecurityGroupAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.DbSecurityGroupNotFoundException;
import software.amazon.awssdk.services.rds.model.EC2SecurityGroup;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceResponse;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.delay.Constant;

public abstract class BaseHandlerStd extends BaseHandler<CallbackContext> {

    protected static final Constant BACK_OFF = Constant.of().timeout(Duration.ofSeconds(150L))
            .delay(Duration.ofSeconds(5L)).build();

    protected static boolean compareEC2SecurityGroupToIngress(
            final EC2SecurityGroup group,
            final Ingress ingress
    ) {
        return Objects.equals(group.ec2SecurityGroupId(), ingress.getEC2SecurityGroupId()) &&
                Objects.equals(group.ec2SecurityGroupName(), ingress.getEC2SecurityGroupName()) &&
                Objects.equals(group.ec2SecurityGroupOwnerId(), ingress.getEC2SecurityGroupOwnerId());
    }

    @Override
    public final ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final Logger logger
    ) {
        return handleRequest(
                proxy,
                request,
                callbackContext != null ? callbackContext : new CallbackContext(),
                proxy.newProxy(ClientBuilder::getClient),
                logger
        );
    }

    protected abstract ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger);

    protected ProgressEvent<ResourceModel, CallbackContext> updateIngresses(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Collection<Ingress> desiredIngresses
    ) {
        if (progress.getCallbackContext().isIngresModified()) {
            return progress;
        }

        return proxy.initiate("rds::update-ingress", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(Translator::describeDBSecurityGroupsRequest)
                .makeServiceCall((request, proxyInvocation) -> {
                    return proxyInvocation.injectCredentialsAndInvokeV2(
                            request,
                            proxyInvocation.client()::describeDBSecurityGroups
                    );
                })
                .handleError((request, exception, client, model, context) -> handleException(
                        ProgressEvent.progress(model, context),
                        exception
                ))
                .done((request, response, client, model, context) -> {
                    final DBSecurityGroup dbSecurityGroup = response.dbSecurityGroups().stream().findFirst().get();

                    final Set<Ingress> currentIngresses = dbSecurityGroup
                            .ec2SecurityGroups()
                            .stream()
                            .map(Translator::translateDBSecurityGroupIngressFromSdk)
                            .collect(Collectors.toSet());

                    final Collection<Ingress> ingressToAdd = new ArrayList<>(desiredIngresses);
                    final Collection<Ingress> ingressToRemove = new ArrayList<>(currentIngresses);

                    ingressToRemove.removeAll(desiredIngresses);
                    ingressToAdd.removeAll(currentIngresses);

                    return ProgressEvent.progress(model, context)
                            .then(p -> revokeIngresses(proxy, proxyClient, p, ingressToRemove))
                            .then(p -> authorizeIngresses(proxy, proxyClient, p, ingressToAdd));
                });
    }

    protected ProgressEvent<ResourceModel, CallbackContext> authorizeIngresses(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Collection<Ingress> ingressesToAuthorize
    ) {
        for (final Ingress ingress : ingressesToAuthorize) {
            final ProgressEvent<ResourceModel, CallbackContext> progressEvent = proxy.initiate("rds::authorize-db-security-group-ingress", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                    .translateToServiceRequest(request -> Translator.authorizeDBSecurityGroupIngresRequest(progress.getResourceModel(), ingress))
                    .makeServiceCall((request, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                            request,
                            proxyInvocation.client()::authorizeDBSecurityGroupIngress)
                    )
                    .stabilize((request, response, proxyInvocation, model, callbackContext) -> isDBSecurityGroupIngressAuthorized(
                            proxyInvocation, model, ingress
                    ))
                    .success();
            if (!progressEvent.isSuccess()) {
                return progressEvent;
            }
        }

        return ProgressEvent.progress(progress.getResourceModel(), progress.getCallbackContext());
    }

    protected ProgressEvent<ResourceModel, CallbackContext> revokeIngresses(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Collection<Ingress> ingressesToRevoke
    ) {
        for (final Ingress ingress : ingressesToRevoke) {
            final ProgressEvent<ResourceModel, CallbackContext> progressEvent = proxy.initiate("rds::revoke-db-security-group-ingress", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                    .translateToServiceRequest(request -> Translator.revokeDBSecurityGroupIngresRequest(progress.getResourceModel(), ingress))
                    .makeServiceCall((request, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                            request,
                            proxyInvocation.client()::revokeDBSecurityGroupIngress)
                    )
                    .stabilize((request, response, proxyInvocation, model, callbackContext) -> isDBSecurityGroupIngressRevoked(
                            proxyInvocation, model, ingress
                    ))
                    .success();
            if (!progressEvent.isSuccess()) {
                return progressEvent;
            }
        }

        return ProgressEvent.progress(progress.getResourceModel(), progress.getCallbackContext());
    }

    protected List<Tag> listTags(
            final ProxyClient<RdsClient> proxyClient,
            final String arn
    ) {
        final ListTagsForResourceResponse listTagsForResourceResponse = proxyClient.injectCredentialsAndInvokeV2(
                Translator.listTagsForResourceRequest(arn),
                proxyClient.client()::listTagsForResource
        );
        return Translator.translateTagsFromSdk(listTagsForResourceResponse.tagList());
    }

    protected ProgressEvent<ResourceModel, CallbackContext> updateTags(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Collection<Tag> previousTags,
            final Collection<Tag> desiredTags
    ) {
        return proxy.initiate("rds::tag-db-security-group", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(Translator::describeDBSecurityGroupsRequest)
                .makeServiceCall((request, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        request,
                        proxyInvocation.client()::describeDBSecurityGroups
                ))
                .handleError((request, exception, client, model, context) -> handleException(
                        ProgressEvent.progress(model, context),
                        exception
                ))
                .done((request, response, proxyInvocation, model, context) -> {
                    final DBSecurityGroup dbSecurityGroup = response.dbSecurityGroups().stream().findFirst().get();

                    final Set<Tag> tagsToAdd = new HashSet<>(desiredTags);
                    final Set<Tag> tagsToRemove = new HashSet<>(previousTags);

                    tagsToAdd.removeAll(previousTags);
                    tagsToRemove.removeAll(desiredTags);

                    removeOldTags(proxyClient, dbSecurityGroup.dbSecurityGroupArn(), tagsToRemove);
                    addNewTags(proxyClient, dbSecurityGroup.dbSecurityGroupArn(), tagsToAdd);

                    return ProgressEvent.progress(progress.getResourceModel(), progress.getCallbackContext());
                });
    }

    protected void removeOldTags(
            final ProxyClient<RdsClient> proxyClient,
            final String arn,
            final Collection<Tag> tagsToRemove
    ) {
        if (CollectionUtils.isNullOrEmpty(tagsToRemove)) {
            return;
        }
        proxyClient.injectCredentialsAndInvokeV2(
                Translator.removeTagsFromResourceRequest(arn, tagsToRemove),
                proxyClient.client()::removeTagsFromResource
        );
    }

    protected void addNewTags(
            final ProxyClient<RdsClient> proxyClient,
            final String arn,
            final Collection<Tag> tagsToAdd
    ) {
        if (CollectionUtils.isNullOrEmpty(tagsToAdd)) {
            return;
        }
        proxyClient.injectCredentialsAndInvokeV2(
                Translator.addTagsToResourceRequest(arn, tagsToAdd),
                proxyClient.client()::addTagsToResource
        );
    }

    protected Boolean isDBSecurityGroupDeleted(
            final ResourceModel model,
            final ProxyClient<RdsClient> proxyInvocation
    ) {
        try {
            proxyInvocation.injectCredentialsAndInvokeV2(
                    Translator.describeDBSecurityGroupsRequest(model),
                    proxyInvocation.client()::describeDBSecurityGroups
            );
            return false;
        } catch (DbSecurityGroupNotFoundException e) {
            return true;
        }
    }

    protected Boolean isDBSecurityGroupStabilized(
            final ResourceModel model,
            final ProxyClient<RdsClient> proxyInvocation
    ) {
        try {
            proxyInvocation.injectCredentialsAndInvokeV2(
                    Translator.describeDBSecurityGroupsRequest(model),
                    proxyInvocation.client()::describeDBSecurityGroups
            );
            return true;
        } catch (DbSecurityGroupNotFoundException e) {
            return false;
        }
    }

    protected EC2SecurityGroup getIngressEC2SecurityGroup(
            final ProxyClient<RdsClient> proxyClient,
            final ResourceModel model,
            final Ingress ingress
    ) {
        final Optional<DBSecurityGroup> dbSecurityGroup = proxyClient.injectCredentialsAndInvokeV2(
                Translator.describeDBSecurityGroupsRequest(model),
                proxyClient.client()::describeDBSecurityGroups
        ).dbSecurityGroups().stream().findFirst();

        if (!dbSecurityGroup.isPresent()) {
            throw new CfnNotFoundException(ResourceModel.TYPE_NAME, model.getGroupName());
        }

        return dbSecurityGroup.get()
                .ec2SecurityGroups()
                .stream()
                .filter(group -> compareEC2SecurityGroupToIngress(group, ingress)).findFirst().orElse(null);
    }

    protected Boolean isDBSecurityGroupIngressAuthorized(
            final ProxyClient<RdsClient> proxyClient,
            final ResourceModel model,
            final Ingress ingress
    ) {
        final EC2SecurityGroup ec2SecurityGroup = getIngressEC2SecurityGroup(proxyClient, model, ingress);
        if (ec2SecurityGroup == null) {
            return false;
        }
        return IngressStatus.Authorized.equalsString(ec2SecurityGroup.status());
    }

    protected Boolean isDBSecurityGroupIngressRevoked(
            ProxyClient<RdsClient> proxyClient,
            ResourceModel model,
            Ingress ingress
    ) {
        final EC2SecurityGroup ec2SecurityGroup = getIngressEC2SecurityGroup(proxyClient, model, ingress);
        if (ec2SecurityGroup == null) {
            return true;
        }
        return IngressStatus.Revoked.equalsString(ec2SecurityGroup.status());
    }

    protected ProgressEvent<ResourceModel, CallbackContext> handleException(
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Exception exception
    ) {
        if (exception instanceof DbSecurityGroupAlreadyExistsException) {
            return ProgressEvent.defaultFailureHandler(exception, HandlerErrorCode.AlreadyExists);
        } else if (exception instanceof DbSecurityGroupNotFoundException) {
            return ProgressEvent.defaultFailureHandler(exception, HandlerErrorCode.NotFound);
        }
        return ProgressEvent.defaultFailureHandler(exception, HandlerErrorCode.InternalFailure);
    }
}
