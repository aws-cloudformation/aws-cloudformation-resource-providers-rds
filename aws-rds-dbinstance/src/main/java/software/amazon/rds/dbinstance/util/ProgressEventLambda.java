package software.amazon.rds.dbinstance.util;

import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.rds.dbinstance.CallbackContext;
import software.amazon.rds.dbinstance.ResourceModel;

public interface ProgressEventLambda {
    ProgressEvent<ResourceModel, CallbackContext> enact();
}
