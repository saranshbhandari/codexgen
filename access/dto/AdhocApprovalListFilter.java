import lombok.*;

@Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
public class AdhocApprovalListFilter {
    private String approverSoeid; // required
    private String soeid;         // optional filter: requester soeid
    private String pid;           // optional filter
    private String status;        // optional filter: GRANTED/PENDINGAPPROVAL/REVOKED
}