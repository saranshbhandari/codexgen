import org.springframework.data.jpa.repository.*;
import org.springframework.data.repository.query.Param;

import java.util.*;

public interface CfeUserAdhocWorkflowAccessRepository extends JpaRepository<CfeUserAdhocWorkflowAccess, Long> {

    Optional<CfeUserAdhocWorkflowAccess> findBySoeidAndWfid(String soeid, Long wfid);

    List<CfeUserAdhocWorkflowAccess> findBySoeid(String soeid);

    @Query("""
        select a.wfid
        from CfeUserAdhocWorkflowAccess a
        join a.workflow w
        where a.soeid = :soeid
          and a.accessStatus = 'GRANTED'
          and w.project.pid = :pid
    """)
    List<Long> findGrantedWfidsBySoeidAndPid(@Param("soeid") String soeid, @Param("pid") String pid);

    @Modifying
    @Query("""
        update CfeUserAdhocWorkflowAccess a
           set a.accessStatus = 'REVOKED',
               a.updatedBy = :updatedBy,
               a.updatedDate = current_date
         where a.soeid = :soeid
           and a.accessStatus = 'GRANTED'
           and a.wfid <> :keepWfid
           and exists (
               select 1
               from CfeWorkflows w
               where w.wfid = a.wfid
                 and w.project.pid = :pid
           )
    """)
    int revokeOtherGrantedForUserInProject(@Param("soeid") String soeid,
                                          @Param("pid") String pid,
                                          @Param("keepWfid") Long keepWfid,
                                          @Param("updatedBy") String updatedBy);

    @Query("""
        select a
        from CfeUserAdhocWorkflowAccess a
        join a.workflow w
        where w.project.pid in :pids
          and (:reqSoeid is null or a.soeid = :reqSoeid)
          and (:pid is null or w.project.pid = :pid)
          and (:status is null or a.accessStatus = :status)
        order by w.project.pid, a.createdDate desc
    """)
    List<CfeUserAdhocWorkflowAccess> approvalList(@Param("pids") List<String> pids,
                                                 @Param("reqSoeid") String reqSoeid,
                                                 @Param("pid") String pid,
                                                 @Param("status") String status);
}