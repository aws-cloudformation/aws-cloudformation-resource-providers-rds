package software.amazon.rds.dbsecuritygroup;

import java.util.Collection;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class UpdateHandler extends BaseHandlerStd {

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger) {

        final Collection<Tag> desiredTags = Translator.translateTagsFromRequest(request.getDesiredResourceTags());
        final Collection<Tag> previousTags = Translator.translateTagsFromRequest(request.getPreviousResourceTags());
        final Collection<Ingress> desiredIngresses = request.getDesiredResourceState().getDBSecurityGroupIngress();

        return ProgressEvent.progress(request.getDesiredResourceState(), callbackContext)
                .then(progress -> updateIngresses(proxy, proxyClient, progress, desiredIngresses))
                .then(progress -> updateTags(proxy, proxyClient, progress, previousTags, desiredTags))
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }
}
