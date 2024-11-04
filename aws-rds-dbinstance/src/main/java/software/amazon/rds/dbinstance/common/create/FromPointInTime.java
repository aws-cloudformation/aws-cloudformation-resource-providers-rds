package software.amazon.rds.dbinstance.common.create;

import org.apache.commons.lang3.NotImplementedException;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.rds.dbinstance.CallbackContext;
import software.amazon.rds.dbinstance.ResourceModel;

public class FromPointInTime implements DBInstanceFactory {

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> create(ProgressEvent<ResourceModel, CallbackContext> input) {
        throw new NotImplementedException();
    }
}
