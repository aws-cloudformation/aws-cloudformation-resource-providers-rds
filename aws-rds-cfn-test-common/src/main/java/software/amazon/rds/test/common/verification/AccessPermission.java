package software.amazon.rds.test.common.verification;

import lombok.AllArgsConstructor;
import lombok.Data;
import software.amazon.rds.test.common.core.ServiceProvider;

@Data
@AllArgsConstructor
public class AccessPermission {
    private ServiceProvider serviceProvider;
    private String methodName;

    @Override
    public String toString() {
        return String.format("%s:%s", serviceProvider.toString(), methodName);
    }
}
