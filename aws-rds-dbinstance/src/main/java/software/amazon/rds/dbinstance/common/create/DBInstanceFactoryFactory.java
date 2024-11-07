package software.amazon.rds.dbinstance.common.create;

import lombok.AllArgsConstructor;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.common.logging.RequestLogger;
import software.amazon.rds.dbinstance.CallbackContext;
import software.amazon.rds.dbinstance.ResourceModel;
import software.amazon.rds.dbinstance.client.ApiVersionDispatcher;
import software.amazon.rds.dbinstance.client.VersionedProxyClient;
import software.amazon.rds.dbinstance.util.ResourceModelHelper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;

@AllArgsConstructor
public class DBInstanceFactoryFactory {

    private final Collection<DBInstanceFactory> factories;
    private final DBInstanceFactory defaultFactory;

    public DBInstanceFactoryFactory(
            final AmazonWebServicesClientProxy proxy,
            final VersionedProxyClient<RdsClient> rdsProxyClient,
            final Tagging.TagSet allTags,
            final RequestLogger requestLogger,
            final HandlerConfig config,
            final ApiVersionDispatcher<ResourceModel, CallbackContext> apiVersionDispatcher
    ) {

        factories = new ArrayList<>();
        // The order of this list matters. Do NOT re-order.
        factories.add(new FromPointInTime(
                proxy,
                rdsProxyClient,
                allTags,
                requestLogger,
                config
        ));
        factories.add(new ReadReplica(
                proxy,
                rdsProxyClient,
                allTags,
                requestLogger,
                config
        ));
        factories.add(new FromSnapshot(
                proxy,
                rdsProxyClient,
                allTags,
                requestLogger,
                config,
                apiVersionDispatcher
        ));

        defaultFactory = new FreshInstance(
                proxy,
                rdsProxyClient,
                allTags,
                requestLogger,
                config,
                apiVersionDispatcher
        );
    }

    public DBInstanceFactory createFactory(ResourceModel model) {
        return discernFactoryType(model).orElse(defaultFactory);
    }

    private Optional<DBInstanceFactory> discernFactoryType(ResourceModel model) {
        for (DBInstanceFactory fac : factories) {
            if (fac.modelSatisfiesConstructor(model)) {
               return Optional.of(fac);
            }
        }
        return Optional.empty();
    }
}
