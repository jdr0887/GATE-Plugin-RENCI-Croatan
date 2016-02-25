package org.renci.gate.service.croatan;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.Queue;
import org.renci.jlrm.Site;
import org.renci.jlrm.commons.ssh.SSHConnectionUtil;
import org.renci.jlrm.pbs.ssh.PBSSSHJob;
import org.renci.jlrm.pbs.ssh.PBSSubmitScriptExporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CroatanSubmitCondorGlideinCallable implements Callable<PBSSSHJob> {

    private final Logger logger = LoggerFactory.getLogger(CroatanSubmitCondorGlideinCallable.class);

    private Site site;

    private Queue queue;

    private PBSSSHJob job;

    private File submitDir;

    private String collectorHost;

    private Integer requiredMemory;

    private String hostAllowRead;

    private String hostAllowWrite;

    private String username;

    private String numberOfProcessors = "$(DETECTED_CORES)";

    public CroatanSubmitCondorGlideinCallable() {
        super();
    }

    /**
     * Creates a new glide-in
     * 
     * @return number of glide-ins submitted
     */
    public PBSSSHJob call() throws JLRMException {
        logger.debug("ENTERING call()");

        try {
            Properties velocityProperties = new Properties();
            velocityProperties.put("runtime.log.logsystem.class", "org.apache.velocity.runtime.log.NullLogChute");
            Velocity.init(velocityProperties);
        } catch (Exception e) {
            e.printStackTrace();
        }

        PBSSSHJob job = new PBSSSHJob();
        job.setTransferExecutable(Boolean.TRUE);
        job.setTransferInputs(Boolean.TRUE);
        job.setQueueName(this.queue.getName());
        job.setName("glidein");
        job.setHostCount(1);
        job.setNumberOfProcessors(getQueue().getNumberOfProcessors());
        job.setOutput(new File("glidein.out"));
        job.setError(new File("glidein.err"));
        job.setWallTime(this.queue.getRunTime());
        job.setMemory(null);

        VelocityContext velocityContext = new VelocityContext();
        velocityContext.put("siteName", getSite().getSubmitHost());
        velocityContext.put("collectorHost", this.collectorHost);
        velocityContext.put("jlrmUser", this.username);
        velocityContext.put("jlrmSiteName", getSite().getName());
        velocityContext.put("hostAllowRead", this.hostAllowRead);
        velocityContext.put("hostAllowWrite", this.hostAllowWrite);
        velocityContext.put("numberOfProcessors", this.numberOfProcessors);

        // note that we want a lower max run time here, so that the glidein can shut down
        // gracefully before getting kicked off by the batch scheduler
        long maxRunTimeAdjusted = this.queue.getRunTime() - 20;
        if (maxRunTimeAdjusted < 0) {
            maxRunTimeAdjusted = this.queue.getRunTime() / 2;
        }
        velocityContext.put("siteMaxRunTimeMins", maxRunTimeAdjusted);
        velocityContext.put("siteMaxRunTimeSecs", maxRunTimeAdjusted * 60);
        velocityContext.put("requiredMemory", this.requiredMemory * 1024);
        velocityContext.put("glideinStartTime", new Date().getTime());
        velocityContext.put("maxRunTime", maxRunTimeAdjusted);

        try {
            String remoteWorkDirSuffix = String.format(".jlrm/jobs/%s/%s",
                    DateFormatUtils.ISO_DATE_FORMAT.format(new Date()), UUID.randomUUID().toString());
            String command = String.format("(mkdir -p $HOME/%s && echo $HOME)", remoteWorkDirSuffix);
            String remoteHome = SSHConnectionUtil.execute(command, site.getUsername(), getSite().getSubmitHost());

            logger.info("remoteHome: {}", remoteHome);
            String remoteWorkDir = String.format("%s/%s", remoteHome, remoteWorkDirSuffix);
            logger.info("remoteWorkDir: {}", remoteWorkDir);
            velocityContext.put("remoteWorkDir", remoteWorkDir);

            File tmpDir = new File(System.getProperty("java.io.tmpdir"));
            File myDir = new File(tmpDir, System.getProperty("user.name"));
            File localWorkDir = new File(myDir, UUID.randomUUID().toString());
            localWorkDir.mkdirs();
            logger.info("localWorkDir: {}", localWorkDir.getAbsolutePath());

            String glideinScriptMacro = IOUtils.toString(this.getClass().getResourceAsStream("glidein.sh.vm"));
            File glideinScript = new File(localWorkDir.getAbsolutePath(), "glidein.sh");
            writeTemplate(velocityContext, glideinScript, glideinScriptMacro);
            job.setExecutable(glideinScript);

            String condorConfigLocalScriptMacro = IOUtils
                    .toString(this.getClass().getResourceAsStream("condor_config.local.vm"));
            File condorConfigLocal = new File(localWorkDir.getAbsolutePath(), "condor_config.local");
            writeTemplate(velocityContext, condorConfigLocal, condorConfigLocalScriptMacro);
            job.getInputFiles().add(condorConfigLocal);

            String condorConfigScriptMacro = IOUtils.toString(this.getClass().getResourceAsStream("condor_config"));
            File condorConfig = new File(localWorkDir.getAbsolutePath(), "condor_config");
            writeTemplate(velocityContext, condorConfig, condorConfigScriptMacro);
            job.getInputFiles().add(condorConfig);

            PBSSubmitScriptExporter<PBSSSHJob> exporter = new PBSSubmitScriptExporter<PBSSSHJob>();
            this.job = exporter.export(localWorkDir, remoteWorkDir, job);

            SSHConnectionUtil.transferSubmitScript(site.getUsername(), site.getSubmitHost(), remoteWorkDir,
                    this.job.getTransferExecutable(), this.job.getExecutable(), this.job.getTransferInputs(),
                    this.job.getInputFiles(), job.getSubmitFile());

            String targetFile = String.format("%s/%s", remoteWorkDir, job.getSubmitFile().getName());

            command = String.format("qsub %s", targetFile);
            String submitOutput = SSHConnectionUtil.execute(command, site.getUsername(), site.getSubmitHost());

            LineNumberReader lnr = new LineNumberReader(new StringReader(submitOutput));
            String line;
            while ((line = lnr.readLine()) != null) {
                job.setId(line.substring(0, line.indexOf(".")));
            }
        } catch (FileNotFoundException e) {
            logger.error("FileNotFoundException", e);
            throw new JLRMException("JSchException: " + e.getMessage());
        } catch (IOException e) {
            logger.error("IOException", e);
            throw new JLRMException("IOException: " + e.getMessage());
        }

        return job;
    }

    /**
     * Generates a new job file which is used to setup and start the glidein
     */
    private void writeTemplate(VelocityContext velocityContext, File file, String template) throws IOException {
        StringWriter sw = new StringWriter();
        Velocity.evaluate(velocityContext, sw, "glideins", template);
        file.setReadable(true);
        file.setExecutable(true);
        file.setWritable(true, true);
        FileUtils.writeStringToFile(file, sw.toString());
    }

    public Site getSite() {
        return site;
    }

    public void setSite(Site site) {
        this.site = site;
    }

    public Queue getQueue() {
        return queue;
    }

    public void setQueue(Queue queue) {
        this.queue = queue;
    }

    public PBSSSHJob getJob() {
        return job;
    }

    public void setJob(PBSSSHJob job) {
        this.job = job;
    }

    public File getSubmitDir() {
        return submitDir;
    }

    public void setSubmitDir(File submitDir) {
        this.submitDir = submitDir;
    }

    public String getCollectorHost() {
        return collectorHost;
    }

    public void setCollectorHost(String collectorHost) {
        this.collectorHost = collectorHost;
    }

    public Integer getRequiredMemory() {
        return requiredMemory;
    }

    public void setRequiredMemory(Integer requiredMemory) {
        this.requiredMemory = requiredMemory;
    }

    public String getHostAllowRead() {
        return hostAllowRead;
    }

    public void setHostAllowRead(String hostAllowRead) {
        this.hostAllowRead = hostAllowRead;
    }

    public String getHostAllowWrite() {
        return hostAllowWrite;
    }

    public void setHostAllowWrite(String hostAllowWrite) {
        this.hostAllowWrite = hostAllowWrite;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getNumberOfProcessors() {
        return numberOfProcessors;
    }

    public void setNumberOfProcessors(String numberOfProcessors) {
        this.numberOfProcessors = numberOfProcessors;
    }

}
