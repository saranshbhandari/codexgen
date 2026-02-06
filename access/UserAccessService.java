import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class UserAccessService {

    private final CfeUserAccessRepository userAccessRepo;
    private final CfeWorkflowsRepository workflowsRepo;
    private final CfeUserAdhocWorkflowAccessRepository adhocRepo;

    // ---------------------------
    // 1) GetUserAccessInfo
    // ---------------------------
    @Transactional(readOnly = true)
    public APIResponse<List<UserAccessInfoDTO>> getUserAccessInfo(String soeid) {
        List<CfeUserAccess> accessRows = userAccessRepo.findBySoeidAndIsActive(soeid, "Y");

        // group by pid (because "There could be multiple pid records for one soeid")
        Map<String, List<CfeUserAccess>> byPid = accessRows.stream()
                .collect(Collectors.groupingBy(CfeUserAccess::getPid));

        List<UserAccessInfoDTO> result = new ArrayList<>();

        for (Map.Entry<String, List<CfeUserAccess>> e : byPid.entrySet()) {
            String pid = e.getKey();
            // pick first row (same pid should have same role & project typically)
            CfeUserAccess ua = e.getValue().get(0);

            String pname = ua.getProject() != null ? ua.getProject().getPname() : null;
            CfeUserRoles role = ua.getRole();

            List<Long> grantedWfids = adhocRepo.findGrantedWfidsBySoeidAndPid(soeid, pid);

            result.add(UserAccessInfoDTO.builder()
                    .pid(pid)
                    .pname(pname)
                    .roleId(role != null ? role.getRoleId() : ua.getRoleId())
                    .roleName(role != null ? role.getRoleName() : null)
                    .hasReadAccess(role != null ? role.getHasReadAccess() : null)
                    .hasWriteAccess(role != null ? role.getHasWriteAccess() : null)
                    .hasSpecificWriteAccess(role != null ? role.getHasSpecificWriteAccess() : null)
                    .hasProjectSettingsAccess(role != null ? role.getHasProjectSettingsAccess() : null)
                    .hasAdminAccess(role != null ? role.getHasAdminScreensAccess() : null)
                    .grantedAdhocWfids(grantedWfids)
                    .build());
        }

        result.sort(Comparator.comparing(UserAccessInfoDTO::getPid, Comparator.nullsLast(String::compareTo)));
        return APIResponse.ok("User access info fetched", result);
    }

    // ---------------------------
    // 2) RequestAdhocWorkflowAccess (upsert)
    // ---------------------------
    @Transactional
    public APIResponse<CfeUserAdhocWorkflowAccess> requestAdhocWorkflowAccess(RequestAdhocWorkflowAccessRequest req) {
        String soeid = req.getSoeid();
        Long wfid = req.getWfid();

        CfeWorkflows wf = workflowsRepo.findById(wfid)
                .orElseThrow(() -> new IllegalArgumentException("Workflow not found for wfid=" + wfid));

        CfeProjects project = wf.getProject();
        if (project == null) {
            throw new IllegalStateException("Workflow has no project mapping for wfid=" + wfid);
        }

        boolean autoApprove = "Y".equalsIgnoreCase(project.getAutoApproveEditAccess());
        boolean allowMultiple = "Y".equalsIgnoreCase(project.getAllowMultipleEdits());

        AccessStatus newStatus = autoApprove ? AccessStatus.GRANTED : AccessStatus.PENDINGAPPROVAL;

        CfeUserAdhocWorkflowAccess entity = adhocRepo.findBySoeidAndWfid(soeid, wfid)
                .orElseGet(CfeUserAdhocWorkflowAccess::new);

        boolean isNew = (entity.getId() == null);

        entity.setSoeid(soeid);
        entity.setWfid(wfid);
        entity.setAccessStatus(newStatus.name());
        entity.setDescription(req.getDescription());

        if (isNew) {
            entity.setCreatedBy(soeid);
            entity.setCreatedDate(LocalDate.now());
        }
        entity.setUpdatedBy(soeid);
        entity.setUpdatedDate(LocalDate.now());

        CfeUserAdhocWorkflowAccess saved = adhocRepo.save(entity);

        // If auto approved and allowMultipleEdits = N => revoke other granted records for same soeid in SAME PROJECT
        if (autoApprove && !allowMultiple) {
            int revoked = adhocRepo.revokeOtherGrantedForUserInProject(
                    soeid,
                    project.getPid(),
                    wfid,
                    soeid
            );
            log.info("Auto-approved adhoc access. allowMultipleEdits=N => revoked {} other GRANTED records for soeid={}, pid={}",
                    revoked, soeid, project.getPid());
        }

        return APIResponse.ok("Adhoc workflow access requested: " + newStatus, saved);
    }

    // ---------------------------
    // Helpers: authorization for approver actions
    // ---------------------------
    private boolean canApproveForProject(String approverSoeid, String pid) {
        List<String> approverPids = userAccessRepo.findProjectIdsWhereUserCanApprove(approverSoeid);
        return approverPids.contains(pid);
    }

    private String getPidForAdhocRecord(CfeUserAdhocWorkflowAccess a) {
        // best: use relationship a.getWorkflow().getProject().getPid()
        if (a.getWorkflow() != null && a.getWorkflow().getProject() != null) {
            return a.getWorkflow().getProject().getPid();
        }
        // fallback: load workflow
        CfeWorkflows wf = workflowsRepo.findById(a.getWfid())
                .orElseThrow(() -> new IllegalArgumentException("Workflow not found for wfid=" + a.getWfid()));
        if (wf.getProject() == null) throw new IllegalStateException("Workflow has no project mapping for wfid=" + a.getWfid());
        return wf.getProject().getPid();
    }

    // ---------------------------
    // 3) ApprovedAdhocWorkflowAccess
    // ---------------------------
    @Transactional
    public APIResponse<CfeUserAdhocWorkflowAccess> approveAdhocWorkflowAccess(Long id, String approverSoeid) {
        CfeUserAdhocWorkflowAccess a = adhocRepo.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("Adhoc access record not found: id=" + id));

        // enforce: only PENDINGAPPROVAL can be approved
        if (!AccessStatus.PENDINGAPPROVAL.name().equalsIgnoreCase(a.getAccessStatus())) {
            return APIResponse.fail("Only PENDINGAPPROVAL can be approved. Current status=" + a.getAccessStatus());
        }

        String pid = getPidForAdhocRecord(a);

        if (!canApproveForProject(approverSoeid, pid)) {
            return APIResponse.fail("User is not authorized to approve for pid=" + pid);
        }

        a.setAccessStatus(AccessStatus.GRANTED.name());
        a.setUpdatedBy(approverSoeid);
        a.setUpdatedDate(LocalDate.now());

        CfeUserAdhocWorkflowAccess saved = adhocRepo.save(a);
        return APIResponse.ok("Adhoc workflow access approved", saved);
    }

    // ---------------------------
    // 4) RevokeAdhocWorkflowAccess
    // ---------------------------
    @Transactional
    public APIResponse<CfeUserAdhocWorkflowAccess> revokeAdhocWorkflowAccess(Long id, String actorSoeid) {
        CfeUserAdhocWorkflowAccess a = adhocRepo.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("Adhoc access record not found: id=" + id));

        // If requester revokes their own: allow
        if (!actorSoeid.equalsIgnoreCase(a.getSoeid())) {
            String pid = getPidForAdhocRecord(a);
            if (!canApproveForProject(actorSoeid, pid)) {
                return APIResponse.fail("User is not authorized to revoke for pid=" + pid);
            }
        }

        a.setAccessStatus(AccessStatus.REVOKED.name());
        a.setUpdatedBy(actorSoeid);
        a.setUpdatedDate(LocalDate.now());

        CfeUserAdhocWorkflowAccess saved = adhocRepo.save(a);
        return APIResponse.ok("Adhoc workflow access revoked", saved);
    }

    // ---------------------------
    // 5) GetMyAdhocworkflowAccess
    // ---------------------------
    @Transactional(readOnly = true)
    public APIResponse<List<CfeUserAdhocWorkflowAccess>> getMyAdhocworkflowAccess(String soeid) {
        return APIResponse.ok("My adhoc workflow access fetched", adhocRepo.findBySoeid(soeid));
    }

    // ---------------------------
    // 6) GetAdhocworkflowAccessApprovalList (approver scope + optional filters)
    // ---------------------------
    @Transactional(readOnly = true)
    public APIResponse<List<CfeUserAdhocWorkflowAccess>> getAdhocworkflowAccessApprovalList(AdhocApprovalListFilter filter) {
        List<String> approverPids = userAccessRepo.findProjectIdsWhereUserCanApprove(filter.getApproverSoeid());
        if (approverPids == null || approverPids.isEmpty()) {
            return APIResponse.ok("No approval scope for this user", Collections.emptyList());
        }

        // status filter normalization
        String status = filter.getStatus();
        if (status != null && !status.isBlank()) {
            status = status.trim().toUpperCase(Locale.ROOT);
        } else {
            status = null;
        }

        List<CfeUserAdhocWorkflowAccess> list = adhocRepo.approvalList(
                approverPids,
                (filter.getSoeid() == null || filter.getSoeid().isBlank()) ? null : filter.getSoeid(),
                (filter.getPid() == null || filter.getPid().isBlank()) ? null : filter.getPid(),
                status
        );

        return APIResponse.ok("Approval list fetched", list);
    }
}