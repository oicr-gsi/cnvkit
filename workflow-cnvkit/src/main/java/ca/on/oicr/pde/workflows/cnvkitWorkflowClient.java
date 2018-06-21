
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

    //cnvkit intermediate file names
    private String bamFile;
   // private String bambaiFile;
   // private String targetcoverageFile;
  //  private String antiTargetCoverageFile;
   // private String cnrFile;
  //  private String cnsFile;
 //   private String scatterPDFFile;
    private String scatterPNGFile;
   // private String diagramPDFFile;
    private String segmentricscnsFile;
    private String segmetricsCallcnsFile;
    //private String segmetricsCallDiagramPDFFile;
    
    
    // Output check
//    private boolean isFolder = true;
    //Scripts 
//    private String sequenzaUtil;
//    private String sequenzaRscript;
 //   private String sequenzav2Script;
    
    //Tools
    private String python;
    private String rpath;
    private String pythonexports;
    private String rexports;
   

    //Memory allocation
    private Integer cnvkitMem;
   
    //path to bin
   // private String bin;
//    private String pypy;
  //  private String rScript;
  //  private String rLib;


    private boolean manualOutput;
    private static final Logger logger = Logger.getLogger(cnvkitWorkflowClient.class.getName());
    private String queue;
    private Map<String, SqwFile> tempFiles;

    // meta-types
    private final static String TXT_METATYPE = "text/plain";
    private final static String TAR_GZ_METATYPE = "application/tar-gzip";

    private void init() {
        try {
            //dir
            dataDir = "data";
            tmpDir = getProperty("tmp_dir");

            // input samples 
            tumor = getProperty("input_files_tumor");
            normal = getProperty("input_files_normal");

            //Ext id
            outputFilenamePrefix = getProperty("external_name");

            //tools
            python = getProperty("PYTHON");
            rpath = getProperty("RPATH");
            pythonexports = "export PATH=" + this.python +":$PATH" + ";" +
                            "export PATH=" + this.python + "/bin" + ":$PATH" + ";" +
                            "export LD_LIBRARY_PATH=" + this.python + "/lib" + ":$LD_LIBRARY_PATH" + ";";
            rexports = "export LD_LIBRARY_PATH=" + this.rpath + "/lib" + ":$LD_LIBRARY_PATH" + ";" +
                       "export PATH=" + this.rpath + ":$PATH" + ";" +
                       "export PATH=" + this.rpath + "/bin" + ":$PATH" + ";" +
                       "export MANPATH=" + this.rpath + "/share/man" + ":$MANPATH" + ";";
           
          
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
         * Steps for sequenza: 1. Check if "bam" file exists; true 2. Check if
         * "bai" file exists; true: go to step 4 3. Check if normal Pb_R sample
         * exists; true: go to step 4; else abort 3. If false: samtools index
         * "bam" file 4. Run job sequenza-utils 5. If outputFile ends with
         * "bin50.gz"; go to step 6; else go to step 4 6. Run job sequenzaR 7.
         * Iterate through the files/folders in outDir: 8. If fileName1 ==
         * "pandc.txt" and fileName2 ends with "Total_CN.seg"; create a folder
         * called "copynumber" 9. If fileType == "folder"; create a folder
         * called "model-fit"; move folders to "model-fit" 10. If fileType ==
         * "file" && fileName != outputFile; move file to "model-fit" 11. Delete
         * outputFile (rm outputFile) 12. zip "model-fit" 13. outputFile =
         * fileName2 14. OutputDir contains the following: fileName1,
         * outputFile, model-fit.zip
         */
        // workflow : read inputs tumor and normal bam; run sequenza-utils; write the output to temp directory; 
        // run sequenzaR; handle output; provision files (3) -- model-fit.zip; text/plain; text/plain
        Job parentJob = null;
        this.outDir = this.outputFilenamePrefix + "_output";
        this.bamFile = this.tmpDir + this.outputFilenamePrefix + this.tumor + ".bam";
      //  this.bambaiFile = this.tmpDir + this.outputFilenamePrefix + ".bam.bai";
      // this.targetcoverageFile = this.tmpDir + this.outputFilenamePrefix + ".targetcoverage.cnn";
      //  this.antiTargetCoverageFile = this.tmpDir + this.outputFilenamePrefix + ".antitargetcoverage.cnn";
      //  this.cnrFile = this.tmpDir + this.outputFilenamePrefix + ".cnr";
      //  this.cnsFile = this.tmpDir + this.outputFilenamePrefix + ".cns";
      //  this.scatterPDFFile = this.tmpDir + this.outputFilenamePrefix + "-scatter.pdf";
        this.scatterPNGFile =  this.tmpDir + this.outputFilenamePrefix + this.tumor + "scatter.png";
      //  this.diagramPDFFile = this.tmpDir + this.outputFilenamePrefix + "-diagram.pdf";
       this.segmentricscnsFile = this.tmpDir + this.outputFilenamePrefix + this.tumor + ".segmetrics.cns";
       this.segmetricsCallcnsFile = this.tmpDir + this.outputFilenamePrefix + this.tumor + ".segmetrics.call.cns";
       // this.segmetricsCallDiagramPDFFile = this.tmpDir + this.outputFilenamePrefix + ".segmetrics.call-diagram.pdf";
        
        
        
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

        // Provision .seg, .varscanSomatic_confints_CP.txt, model-fit.tar.gz files
        String segFile = this.outputFilenamePrefix + ".seg";
        SqwFile cnSegFile = createOutputFile(this.outDir + "/" + segFile, TXT_METATYPE, this.manualOutput);
        cnSegFile.getAnnotations().put("segment data from the tool ", "CNVkit ");
        zipOutput.addFile(cnSegFile);
        
        SqwFile zipFile = createOutputFile(this.outDir + "/" + "model-fit.tar.gz", TAR_GZ_METATYPE, this.manualOutput);
        zipFile.getAnnotations().put("Other files ", "cnvkit ");
        zipOutput.addFile(zipFile);
    }

     private Job iterOutputDir(String outDir) {
        /**
         * Method to handle file from the output directory All provision files
         * are in tempDir Create a directory called model-fit in output
         * directory move the subfolders into it move files with the following
         * extentions to model-fit "_log.txt", ".pdf", "_solutions.txt",
         * "_CP.txt", "_mutations.txt", "segments.txt", ".RData" construct a cmd
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
        cmd.addArgument(this.pythonexports);
        cmd.addArgument(this.rexports);
        cmd.addArgument("cnvkit.py batch " + this.bamFile);
        cmd.addArgument("--reference " + this.normal);
        cmd.addArgument("--scatter");
        cmd.addArgument("--diagram");
        cmd.addArgument("--rlibpath " + this.rpath);
        cmd.addArgument("--output-dir " + this.outDir);    
        batch.setMaxMemory(Integer.toString(cnvkitMem * 1024));
        batch.setQueue(queue);
        return batch;
    }

     private Job runScatterplot() {
        Job scatter = getWorkflow().createBashJob("scatter");
        Command cmd = scatter.getCommand();
        cmd.addArgument(this.pythonexports);
        cmd.addArgument(this.rexports);
        cmd.addArgument("cnvkit.py scatter");
        cmd.addArgument("-s " + this.tumor + ".cn{s,r}");
        cmd.addArgument("-o " + this.scatterPNGFile); 
        scatter.setMaxMemory(Integer.toString(cnvkitMem * 1024));
        scatter.setQueue(queue);
        return scatter;
    }
     
     
      private Job runCalculatesegmetrics() {
        Job segmetrics = getWorkflow().createBashJob("segmetrics");
        Command cmd = segmetrics.getCommand();
        cmd.addArgument(this.pythonexports);
        cmd.addArgument(this.rexports);
        cmd.addArgument("cnvkit.py segmetrics");
        cmd.addArgument("-s " + this.tumor + ".cn{s,r}");
        cmd.addArgument("--ci");
        cmd.addArgument("--pi");
        segmetrics.setMaxMemory(Integer.toString(cnvkitMem * 1024));
        segmetrics.setQueue(queue);
        return segmetrics;
    }

      private Job runFilter() {
        Job filter = getWorkflow().createBashJob("filter");
        Command cmd = filter.getCommand();
        cmd.addArgument(this.pythonexports);
        cmd.addArgument(this.rexports);
        cmd.addArgument("cnvkit.py call");
        cmd.addArgument("--filter cn");
        cmd.addArgument("--filter ci");
        cmd.addArgument(this.segmentricscnsFile);
        filter.setMaxMemory(Integer.toString(cnvkitMem * 1024));
        filter.setQueue(queue);
        return filter;
    }
      
       private Job runCleanupdiagram() {
        Job diagram = getWorkflow().createBashJob("diagram");
        Command cmd = diagram.getCommand();
        cmd.addArgument(this.pythonexports);
        cmd.addArgument(this.rexports);
        cmd.addArgument("cnvkit.py diagram");
        cmd.addArgument("-s " + this.segmetricsCallcnsFile);
        diagram.setMaxMemory(Integer.toString(cnvkitMem * 1024));
        diagram.setQueue(queue);
        return diagram;
    }
}
