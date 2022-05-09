package software.amazon.rds.dbinstance;

import java.util.List;

import com.amazonaws.util.CollectionUtils;
import com.google.common.collect.Lists;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.dbinstance.util.DispatchRoute;

public class UpdateHandler extends BaseHandlerStd {

    private final List<DispatchRoute<BaseUpdateHandler>> dispatchTable;

    public UpdateHandler() {
        this(DEFAULT_DB_INSTANCE_HANDLER_CONFIG);
    }

    public UpdateHandler(final HandlerConfig config) {
        super(config);

        this.dispatchTable = Lists.newArrayList(
                new DispatchRoute<>(
                        req -> !CollectionUtils.isNullOrEmpty(req.getDesiredResourceState().getDBSecurityGroups()),
                        new UpdateHandlerV12(this.config)
                ),
                new DispatchRoute<>(req -> true, new UpdateHandlerV19(this.config))
        );
    }

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProxyClient<Ec2Client> ec2ProxyClient,
            final Logger logger
    ) {
        for (final DispatchRoute<BaseUpdateHandler> route : dispatchTable) {
            if (route.getPredicate().test(request)) {
                return route.getHandler().handleRequest(proxy, request, callbackContext, rdsProxyClient, ec2ProxyClient, logger);
            }
        }
        throw REQUEST_VERSION_ROUTE_MISSING;
    }
}
