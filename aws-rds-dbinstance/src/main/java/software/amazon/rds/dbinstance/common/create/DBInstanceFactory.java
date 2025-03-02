package software.amazon.rds.dbinstance.common.create;

import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.rds.dbinstance.CallbackContext;
import software.amazon.rds.dbinstance.ResourceModel;
import software.amazon.rds.dbinstance.util.ResourceModelHelper;

public interface DBInstanceFactory {

    ProgressEvent<ResourceModel, CallbackContext> create(
            ProgressEvent<ResourceModel, CallbackContext> input
    );

    boolean modelSatisfiesConstructor(ResourceModel model);
}
