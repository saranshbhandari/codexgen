import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/access")
@RequiredArgsConstructor
public class UserAccessController {

    private final UserAccessService service;

    // 1) GetUserAccessInfo
    @GetMapping("/GetUserAccessInfo")
    public APIResponse<List<UserAccessInfoDTO>> getUserAccessInfo(@RequestParam String soeid) {
        return service.getUserAccessInfo(soeid);
    }

    // 2) RequestAdhocWorkflowAccess
    @PostMapping("/RequestAdhocWorkflowAccess")
    public APIResponse<CfeUserAdhocWorkflowAccess> requestAdhocWorkflowAccess(
            @RequestBody RequestAdhocWorkflowAccessRequest req
    ) {
        return service.requestAdhocWorkflowAccess(req);
    }

    // 3) ApprovedAdhocWorkflowAccess
    @PostMapping("/ApprovedAdhocWorkflowAccess")
    public APIResponse<CfeUserAdhocWorkflowAccess> approveAdhocWorkflowAccess(
            @RequestBody ActionAdhocAccessRequest req
    ) {
        return service.approveAdhocWorkflowAccess(req.getId(), req.getSoeid());
    }

    // 4) RevokeAdhocWorkflowAccess
    @PostMapping("/RevokeAdhocWorkflowAccess")
    public APIResponse<CfeUserAdhocWorkflowAccess> revokeAdhocWorkflowAccess(
            @RequestBody ActionAdhocAccessRequest req
    ) {
        return service.revokeAdhocWorkflowAccess(req.getId(), req.getSoeid());
    }

    // 5) GetMyAdhocworkflowAccess
    @GetMapping("/GetMyAdhocworkflowAccess")
    public APIResponse<List<CfeUserAdhocWorkflowAccess>> getMyAdhocworkflowAccess(@RequestParam String soeid) {
        return service.getMyAdhocworkflowAccess(soeid);
    }

    // 6) GetAdhocworkflowAccessApprovalList (supports 3 filters: soeid,pid,status)
    @PostMapping("/GetAdhocworkflowAccessApprovalList")
    public APIResponse<List<CfeUserAdhocWorkflowAccess>> getApprovalList(
            @RequestBody AdhocApprovalListFilter filter
    ) {
        return service.getAdhocworkflowAccessApprovalList(filter);
    }
}