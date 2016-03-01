package org.renci.gate.service.croatan;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;

import org.apache.commons.lang.StringUtils;
import org.renci.gate.AbstractGATEService;
import org.renci.gate.GATEException;
import org.renci.gate.GlideinMetric;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.Queue;
import org.renci.jlrm.commons.ssh.SSHConnectionUtil;
import org.renci.jlrm.pbs.PBSJobStatusInfo;
import org.renci.jlrm.pbs.PBSJobStatusType;
import org.renci.jlrm.pbs.ssh.PBSSSHKillCallable;
import org.renci.jlrm.pbs.ssh.PBSSSHLookupStatusCallable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author jdr0887
 */
public class CroatanGATEService extends AbstractGATEService {

    private final Logger logger = LoggerFactory.getLogger(CroatanGATEService.class);

    public CroatanGATEService() {
        super();
    }

    @Override
    public Boolean isValid() throws GATEException {
        logger.debug("ENTERING isValid()");
        try {
            String results = SSHConnectionUtil.execute("ls /projects/mapseq | wc -l", getSite().getUsername(),
                    getSite().getSubmitHost());
            if (StringUtils.isNotEmpty(results) && Integer.valueOf(results.trim()) > 0) {
                return true;
            }
        } catch (NumberFormatException | JLRMException e) {
            throw new GATEException(e);
        }
        return false;
    }

    @Override
    public List<GlideinMetric> lookupMetrics() throws GATEException {
        logger.debug("ENTERING lookupMetrics()");
        Map<String, GlideinMetric> metricsMap = new HashMap<String, GlideinMetric>();

        List<Queue> queueList = getSite().getQueueList();
        for (Queue queue : queueList) {
            metricsMap.put(queue.getName(), new GlideinMetric(getSite().getName(), queue.getName(), 0, 0));
        }

        try {
            PBSSSHLookupStatusCallable callable = new PBSSSHLookupStatusCallable(getSite());
            Set<PBSJobStatusInfo> jobStatusSet = Executors.newSingleThreadExecutor().submit(callable).get();
            logger.debug("jobStatusSet.size(): {}", jobStatusSet.size());

            if (jobStatusSet != null && jobStatusSet.size() > 0) {
                String jobName = String.format("glidein-%s", getSite().getName().toLowerCase());

                for (PBSJobStatusInfo info : jobStatusSet) {

                    if (!info.getJobName().equals(jobName)) {
                        continue;
                    }

                    switch (info.getType()) {
                        case QUEUED:
                            metricsMap.get(info.getQueue()).incrementPending();
                            break;
                        case RUNNING:
                            metricsMap.get(info.getQueue()).incrementRunning();
                            break;
                    }

                }

            }

        } catch (Exception e) {
            throw new GATEException(e);
        }

        List<GlideinMetric> metricList = new ArrayList<GlideinMetric>();
        metricList.addAll(metricsMap.values());

        return metricList;
    }

    @Override
    public void createGlidein(Queue queue) throws GATEException {
        logger.debug("ENTERING createGlidein(Queue)");

        File submitDir = new File("/tmp", System.getProperty("user.name"));
        submitDir.mkdirs();

        try {
            logger.info("siteInfo: {}", getSite());
            logger.info("queueInfo: {}", queue);
            CroatanSubmitCondorGlideinCallable callable = new CroatanSubmitCondorGlideinCallable();
            callable.setCollectorHost(getCollectorHost());
            callable.setUsername(System.getProperty("user.name"));
            callable.setSite(getSite());
            callable.setJobName(String.format("glidein-%s", getSite().getName().toLowerCase()));
            callable.setQueue(queue);
            callable.setSubmitDir(submitDir);
            callable.setRequiredMemory(40);
            callable.setHostAllowRead(getHostAllow());
            callable.setHostAllowWrite(getHostAllow());
            Executors.newSingleThreadExecutor().submit(callable).get();
        } catch (Exception e) {
            throw new GATEException(e);
        }
    }

    @Override
    public void deleteGlidein(Queue queue) throws GATEException {
        logger.debug("ENTERING deleteGlidein(Queue)");
        try {
            logger.info("siteInfo: {}", getSite());
            logger.info("queueInfo: {}", queue);
            PBSSSHLookupStatusCallable lookupStatusCallable = new PBSSSHLookupStatusCallable(getSite());
            Set<PBSJobStatusInfo> jobStatusSet = Executors.newSingleThreadExecutor().submit(lookupStatusCallable).get();
            Iterator<PBSJobStatusInfo> iter = jobStatusSet.iterator();
            String jobName = String.format("glidein-%s", getSite().getName().toLowerCase());
            while (iter.hasNext()) {
                PBSJobStatusInfo info = iter.next();
                if (!info.getJobName().equals(jobName)) {
                    continue;
                }
                logger.debug("deleting: {}", info.toString());
                PBSSSHKillCallable killCallable = new PBSSSHKillCallable(getSite(), info.getJobId());
                Executors.newSingleThreadExecutor().submit(killCallable).get();
                // only delete one...engine will trigger next deletion
                break;
            }
        } catch (Exception e) {
            throw new GATEException(e);
        }
    }

    @Override
    public void deletePendingGlideins() throws GATEException {
        logger.debug("ENTERING deletePendingGlideins()");
        try {
            PBSSSHLookupStatusCallable lookupStatusCallable = new PBSSSHLookupStatusCallable(getSite());
            Set<PBSJobStatusInfo> jobStatusSet = Executors.newSingleThreadExecutor().submit(lookupStatusCallable).get();
            String jobName = String.format("glidein-%s", getSite().getName().toLowerCase());
            for (PBSJobStatusInfo info : jobStatusSet) {
                if (!info.getJobName().equals(jobName)) {
                    continue;
                }
                if (info.getType().equals(PBSJobStatusType.QUEUED)) {
                    logger.debug("deleting: {}", info.toString());
                    PBSSSHKillCallable killCallable = new PBSSSHKillCallable(getSite(), info.getJobId());
                    Executors.newSingleThreadExecutor().submit(killCallable).get();
                    // throttle the deleteGlidein calls such that SSH doesn't complain
                    Thread.sleep(2000);
                }
            }
        } catch (Exception e) {
            throw new GATEException(e);
        }
    }

}
