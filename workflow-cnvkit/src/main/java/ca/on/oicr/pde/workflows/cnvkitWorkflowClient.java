package ca.on.oicr.pde.workflows;

import ca.on.oicr.pde.utilities.workflows.OicrWorkflow;
import java.util.Map;
import java.util.logging.Logger;
import net.sourceforge.seqware.pipeline.workflowV2.model.Command;
import net.sourceforge.seqware.pipeline.workflowV2.model.Job;
import net.sourceforge.seqware.pipeline.workflowV2.model.SqwFile;

/**
 * <p>
 * For more information on developing workflows, see the documentation at
 * <a href="http://seqware.github.io/docs/6-pipeline/java-workflows/">SeqWare
 * Java Workflows</a>.</p>
 *
 * Quick reference for the order of methods called: 1. setupDirectory 2.
 * setupFiles 3. setupWorkflow 4. setupEnvironment 5. buildWorkflow
 *
 * See the SeqWare API for
 * <a href="http://seqware.github.io/javadoc/stable/apidocs/net/sourceforge/seqware/pipeline/workflowV2/AbstractWorkflowDataModel.html#setupDirectory%28%29">AbstractWorkflowDataModel</a>
 * for more information.
 */
public class cnvkitWorkflowClient extends OicrWorkflow {

    //dir
    private String dataDir, tmpDir;
    private String outDir;

    // Input Data
    private String tumor;
    private String normal;
    private String outputFilenamePrefix;
    private String sampleName;

    //cnvkit intermediate file names
    private String bamFile;
    private String scatterPNGFile;
    private String segmetricscnsFile;
    private String segmetricsCallcnsFile;
    private String filepath;

    //Tools
    private String python;
    private String rpath;
    private String pythonExports;
    private String rExports;

    //Memory allocation
    private Integer cnvkitMem;

    private boolean manualOutput;
    private static final Logger logger = Logger.getLogger(cnvkitWorkflowClient.class.getName());
    private String queue;
    private Map<String, SqwFile> tempFiles;

     // meta-types
    private final static String TXT_METATYPE = "text/plain";
    private final static String TAR_GZ_METATYPE = "application/tar-gzip";
    private static final String FASTQ_GZIP_MIMETYPE = "chemical/seq-na-fastq-gzip";

    private void init() {
        try {
            //dir
            dataDir = "data";
            tmpDir = getProperty("tmp_dir");

            // input samples 
            tumor = getProperty("input_bam_file");
            normal = getProperty("input_files_normal");
            sampleName = getProperty("sample_name");

            //Ext id
            outputFilenamePrefix = getProperty("output_filename_prefix");

            //tools
            python = getProperty("PYTHON");
            rpath = getProperty("RPATH");
            StringBuilder pyB = new StringBuilder();
            pyB.append("export PATH=");
            pyB.append(this.python);
            pyB.append(":$PATH");
            pyB.append(";");
            pyB.append("export PATH=");
            pyB.append(this.python);
            pyB.append("/bin");
            pyB.append(":$PATH");
            pyB.append(";");
            pyB.append("export LD_LIBRARY_PATH=");
            pyB.append(this.python);
            pyB.append("/lib");
            pyB.append(":$LD_LIBRARY_PATH");
            pyB.append(";");   
            pythonExports = pyB.toString(); // new pythonExports
            // CONVERT to pyB pythonExports = "export PATH=" + this.python + ":$PATH" + ";"
//                    + "export PATH=" + this.python + "/bin" + ":$PATH" + ";"
//                    + "export LD_LIBRARY_PATH=" + this.python + "/lib" + ":$LD_LIBRARY_PATH" + ";";
//            
            StringBuilder rB = new StringBuilder();
            rB.append("export LD_LIBRARY_PATH=");
            rB.append(this.rpath);
            rB.append("/lib");
            rB.append(":$LD_LIBRARY_PATH");
            rB.append(";"); 
            rB.append("export PATH=");
            rB.append(this.rpath);
            rB.append(":$PATH" );
            rB.append(";");
            rB.append("export PATH=");
            rB.append(this.rpath);
            rB.append("/bin");
            rB.append(":$PATH");
            rB.append(";");
            rB.append("export MANPATH=");
            rB.append(this.rpath);
            rB.append("/share/man");
            rB.append(":$MANPATH");
            rB.append(";");
            rExports = rB.toString();
            
//            convert to rB and toString()
//            rExports = "export LD_LIBRARY_PATH=" + this.rpath + "/lib" + ":$LD_LIBRARY_PATH" + ";"
//                    + "export PATH=" + this.rpath + ":$PATH" + ";"
//                    + "export PATH=" + this.rpath + "/bin" + ":$PATH" + ";"
//                    + "export MANPATH=" + this.rpath + "/share/man" + ":$MANPATH" + ";";

            //r path
            manualOutput = Boolean.parseBoolean(getProperty("manual_output"));
            queue = getOptionalProperty("queue", "");

            //sequenza
            cnvkitMem = Integer.parseInt(getProperty("cnvkit_mem"));

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setupDirectory() {
        init();
        this.addDirectory(dataDir);
        this.addDirectory(tmpDir);
        if (!dataDir.endsWith("/")) {
            dataDir += "/";
        }
        if (!tmpDir.endsWith("/")) {
            tmpDir += "/";
        }
    }

    @Override
    public Map<String, SqwFile> setupFiles() {
        SqwFile file0 = this.createFile("tumor");
        file0.setSourcePath(tumor);
        file0.setType("application/bam");
        file0.setIsInput(true);
        SqwFile file1 = this.createFile("normal");
        file1.setSourcePath(normal);
        file1.setType("application/cnn");
        file1.setIsInput(true);
        return this.getFiles();
    }

    @Override
    public void buildWorkflow() {

        /**
         * Steps for cnvkit: 
         */
        // workflow : read inputs tumor bam and cnn file; run cnvkit; write the output to temp directory; 
        // run handle output script; provision files (2) -- model-fit.zip; text/plain; 
        Job parentJob = null;
        this.outDir = this.outputFilenamePrefix + "_output";
        this.bamFile = this.tumor;
        this.filepath = this.tmpDir + this.sampleName;
        this.scatterPNGFile = this.filepath + ".scatter.png";
        this.segmetricscnsFile = this.filepath + ".segmetrics.cns";
        this.segmetricsCallcnsFile = this.filepath + ".segmetrics.call.cns";

        Job batch = runPipeline();

        Job scatter = runScatterplot();
        scatter.addParent(batch);

        Job segmetrics = runCalculatesegmetrics();
        segmetrics.addParent(scatter);

        Job filter = runFilter();
        filter.addParent(segmetrics);

        Job diagram = runCleanupdiagram();
        diagram.addParent(filter);

        Job zipOutput = iterOutputDir(this.outDir);
        zipOutput.addParent(diagram);

        // Provision .seg, model-fit.tar.gz files
        String segFile = this.outputFilenamePrefix + ".seg";
        SqwFile cnSegFile = createOutputFile(this.tmpDir + segFile, TXT_METATYPE, this.manualOutput);
        cnSegFile.getAnnotations().put("segment data from the tool ", "CNVkit ");
        zipOutput.addFile(cnSegFile);

        SqwFile zipFile = createOutputFile(this.tmpDir + "model-fit.tar.gz", TAR_GZ_METATYPE, this.manualOutput);
        zipFile.getAnnotations().put("Other files ", "cnvkit ");
        zipOutput.addFile(zipFile);
    }

    private Job iterOutputDir(String outDir) {
        /**
         * Method to handle file from the output directory All provision files
         * are in tempDir Create a directory called model-fit in output
         * directory move the subfolders into it move files with the following
         * extentions to model-fit ".antitargetcoverage.cnn", ".cnr", ".cns",
         * "-diagram.pdf", "-scatter.pdf", ".scatter.png", ".segmetrics.cns",
         * "targetcoverage.cnn", adn construct a cmd
         * string to zip the model-fit folder
         */
        // find only folders in the output Directory
        Job iterOutput = getWorkflow().createBashJob("handle_output");
        Command cmd = iterOutput.getCommand();
        cmd.addArgument("bash -x " + getWorkflowBaseDir() + "/dependencies/handleFile.sh");
        cmd.addArgument(this.outputFilenamePrefix);
        cmd.addArgument(outDir);
        iterOutput.setMaxMemory(Integer.toString(cnvkitMem * 1024));
        iterOutput.setQueue(getOptionalProperty("queue", ""));
        return iterOutput;
    }

    private Job runPipeline() {
        Job batch = getWorkflow().createBashJob("batch");
        Command cmd = batch.getCommand();
        cmd.addArgument(this.pythonExports);
        cmd.addArgument(this.rExports);
        cmd.addArgument("cnvkit.py batch " + this.bamFile);
        cmd.addArgument("--reference " + this.normal);
        cmd.addArgument("--scatter");
        cmd.addArgument("--diagram");
        cmd.addArgument("--rlibpath " + this.rpath);
        cmd.addArgument("--output-dir " + this.tmpDir);
        batch.setMaxMemory(Integer.toString(cnvkitMem * 1024));
        batch.setQueue(queue);
        return batch;
    }

    private Job runScatterplot() {
        Job scatter = getWorkflow().createBashJob("scatter");
        Command cmd = scatter.getCommand();
        cmd.addArgument(this.pythonExports);
        cmd.addArgument(this.rExports);
        cmd.addArgument("cnvkit.py scatter");
        cmd.addArgument("-s " + this.filepath + ".cn{s,r}");
        cmd.addArgument("-o " + this.scatterPNGFile);
        scatter.setMaxMemory(Integer.toString(cnvkitMem * 1024));
        scatter.setQueue(queue);
        return scatter;
    }

    private Job runCalculatesegmetrics() {
        Job segmetrics = getWorkflow().createBashJob("segmetrics");
        Command cmd = segmetrics.getCommand();
        cmd.addArgument(this.pythonExports);
        cmd.addArgument(this.rExports);
        cmd.addArgument("cnvkit.py segmetrics");
        cmd.addArgument("-s " + this.filepath + ".cn{s,r}");
        cmd.addArgument("--ci");
        cmd.addArgument("--pi");
        cmd.addArgument("-o " + this.filepath + ".segmetrics.cns");
        segmetrics.setMaxMemory(Integer.toString(cnvkitMem * 1024));
        segmetrics.setQueue(queue);
        return segmetrics;
    }

    private Job runFilter() {
        Job filter = getWorkflow().createBashJob("filter");
        Command cmd = filter.getCommand();
        cmd.addArgument(this.pythonExports);
        cmd.addArgument(this.rExports);
        cmd.addArgument("cnvkit.py call");
        cmd.addArgument("--filter cn");
        cmd.addArgument("--filter ci");
        cmd.addArgument(this.segmetricscnsFile);
        cmd.addArgument("-o " + this.filepath + ".segmetrics.call.cns");
        filter.setMaxMemory(Integer.toString(cnvkitMem * 1024));
        filter.setQueue(queue);
        return filter;
    }

    private Job runCleanupdiagram() {
        Job diagram = getWorkflow().createBashJob("diagram");
        Command cmd = diagram.getCommand();
        cmd.addArgument(this.pythonExports);
        cmd.addArgument(this.rExports);
        cmd.addArgument("cnvkit.py diagram");
        cmd.addArgument("-s " + this.segmetricsCallcnsFile);
        cmd.addArgument("-o " + this.segmetricsCallcnsFile);
        diagram.setMaxMemory(Integer.toString(cnvkitMem * 1024));
        diagram.setQueue(queue);
        return diagram;
    }
}
