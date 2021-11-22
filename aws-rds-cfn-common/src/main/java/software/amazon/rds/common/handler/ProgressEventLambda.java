package software.amazon.rds.common.handler;

import software.amazon.cloudformation.proxy.ProgressEvent;

public interface ProgressEventLambda<M, C> {
    ProgressEvent<M, C> enact();
}
