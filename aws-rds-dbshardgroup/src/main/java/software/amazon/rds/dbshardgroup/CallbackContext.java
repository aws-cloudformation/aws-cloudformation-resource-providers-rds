package software.amazon.rds.dbshardgroup;

import software.amazon.cloudformation.proxy.StdCallbackContext;

@lombok.Getter
@lombok.Setter
@lombok.ToString
@lombok.EqualsAndHashCode(callSuper = true)
public class CallbackContext extends StdCallbackContext {
    private boolean described;
    private boolean updated;
    private boolean tagged;

    private String dbClusterIdentifier;

    private int waitTime;
}
