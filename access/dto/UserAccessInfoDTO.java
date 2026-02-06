import lombok.*;
import java.util.*;

@Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
public class UserAccessInfoDTO {
    private String pid;
    private String pname;

    private Long roleId;
    private String roleName;

    private String hasReadAccess;
    private String hasWriteAccess;
    private String hasSpecificWriteAccess;
    private String hasProjectSettingsAccess;
    private String hasAdminAccess; // map from hasAdminScreensAccess

    private List<Long> grantedAdhocWfids;
}