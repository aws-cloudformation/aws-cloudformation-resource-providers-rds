package software.amazon.rds.test.common.verification;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public class AccessPermissionAlias {
    @Getter
    private AccessPermission origin;

    @Getter
    private AccessPermission equivalent;
}
