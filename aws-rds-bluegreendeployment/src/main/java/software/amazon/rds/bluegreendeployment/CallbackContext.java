package software.amazon.rds.bluegreendeployment;

import lombok.Getter;
import lombok.Setter;
import software.amazon.cloudformation.proxy.StdCallbackContext;

@lombok.Getter
@lombok.Setter
@lombok.ToString
@lombok.EqualsAndHashCode(callSuper = true)
public class CallbackContext extends StdCallbackContext {

    @Getter
    @Setter
    private String observedStatus;

    @Getter
    @Setter
    private String sourceDBInstanceIdentifier;

    @Getter
    @Setter
    private boolean statusClarified;
}
