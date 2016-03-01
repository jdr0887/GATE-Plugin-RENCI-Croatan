package org.renci.gate.service.croatan;

import java.io.File;

import org.apache.karaf.shell.api.action.Action;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.Option;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.Queue;
import org.renci.jlrm.Site;
import org.renci.jlrm.pbs.ssh.PBSSSHJob;

@Command(scope = "renci-croatan", name = "create-glidein", description = "Create Glidein")
@Service
public class CreateGlideinAction implements Action {

    @Option(name = "--username", required = true, multiValued = false)
    private String username;

    @Option(name = "--submitHost", required = true, multiValued = false)
    private String submitHost;

    @Option(name = "--queueName", required = true, multiValued = false)
    private String queueName;

    @Option(name = "--collectorHost", required = true, multiValued = false)
    private String collectorHost;

    @Option(name = "--hostAllow", required = false, multiValued = false)
    private String hostAllow;

    public CreateGlideinAction() {
        super();
        this.hostAllow = "*.unc.edu";
    }

    @Override
    public Object execute() {
        Site site = new Site();
        site.setName("Croatan");
        site.setSubmitHost(submitHost);
        site.setUsername(username);

        Queue queue = new Queue();
        queue.setName(queueName);
        queue.setRunTime(5760L);
        queue.setNumberOfProcessors(32);

        File submitDir = new File("/tmp");
        try {
            CroatanSubmitCondorGlideinCallable callable = new CroatanSubmitCondorGlideinCallable();
            callable.setSite(site);
            callable.setQueue(queue);
            callable.setSubmitDir(submitDir);
            callable.setCollectorHost(collectorHost);
            callable.setHostAllowRead(hostAllow);
            callable.setHostAllowWrite(hostAllow);
            callable.setRequiredMemory(40);
            callable.setUsername(System.getProperty("user.name"));
            callable.setJobName(String.format("glidein-%s", site.getName().toLowerCase()));

            PBSSSHJob job = callable.call();
            System.out.println(job.getId());
        } catch (JLRMException e) {
            e.printStackTrace();
        }

        return null;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getSubmitHost() {
        return submitHost;
    }

    public void setSubmitHost(String submitHost) {
        this.submitHost = submitHost;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public String getCollectorHost() {
        return collectorHost;
    }

    public void setCollectorHost(String collectorHost) {
        this.collectorHost = collectorHost;
    }

    public String getHostAllow() {
        return hostAllow;
    }

    public void setHostAllow(String hostAllow) {
        this.hostAllow = hostAllow;
    }

}
