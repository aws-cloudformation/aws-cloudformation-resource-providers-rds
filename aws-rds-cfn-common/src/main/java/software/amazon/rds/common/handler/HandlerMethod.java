package software.amazon.rds.common.handler;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;

public interface HandlerMethod<M, C> {
    ProgressEvent<M, C> invoke(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProgressEvent<M, C> progress,
            final Tagging.TagSet tagSet
    );
}
