package software.amazon.rds.dbinstance.common.create;

import lombok.AllArgsConstructor;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.common.logging.RequestLogger;
import software.amazon.rds.dbinstance.CallbackContext;
import software.amazon.rds.dbinstance.ResourceModel;
import software.amazon.rds.dbinstance.client.ApiVersionDispatcher;
import software.amazon.rds.dbinstance.client.VersionedProxyClient;

@AllArgsConstructor
public class DBInstanceFactoryFactory {

    private final AmazonWebServicesClientProxy proxy;
    private final VersionedProxyClient<RdsClient> rdsProxyClient;
    private final Tagging.TagSet allTags;
    private final RequestLogger requestLogger;
    private final HandlerConfig config;
    private final ApiVersionDispatcher<ResourceModel, CallbackContext> apiVersionDispatcher;

    private enum FactoryType {
        IS_READ_REPLICA,
        IS_FROM_SNAPSHOT,
        IS_FROM_POINT_IN_TIME,
        IS_FRESH_INSTANCE;
    }

    public DBInstanceFactory createFactory(
        ProgressEvent<ResourceModel, CallbackContext> progress
    ) {
        switch (discernFactoryType()) {
            case IS_FRESH_INSTANCE:
                return new FreshInstance(
                    proxy,
                    rdsProxyClient,
                    allTags,
                    requestLogger,
                    config,
                    apiVersionDispatcher
                );
        }
        return null;
    }

    private FactoryType discernFactoryType() {
        return FactoryType.IS_FRESH_INSTANCE;
    }
}
