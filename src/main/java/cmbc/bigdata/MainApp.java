package cmbc.bigdata;

import cmbc.bigdata.constants.CONSTANTSUTIL;
import cmbc.bigdata.constants.FILETYPE;
import cmbc.bigdata.constants.PULLMODE;
import cmbc.bigdata.core.Puller;
import cmbc.bigdata.core.Pusher;
import cmbc.bigdata.utils.XMLHandler;
import cmbc.bigdata.utils.ZKUtils;
import org.apache.curator.framework.CuratorFramework;
import org.kohsuke.args4j.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.kohsuke.args4j.ExampleMode.ALL;

/**
 * Created by huangpengcheng on 2016/7/21 0021.
 */
class MainApp {

    private static final Logger logger = LoggerFactory.getLogger(MainApp.class);

    private CuratorFramework client;

    @Option(name = "-t", usage = "File type: HOSTS/XML/UNKNOWN.")
    private FILETYPE fileType=FILETYPE.PLAIN;

    @Option(name = "-pull", usage = "Set the Pull Mode",forbids = {"-push"},depends = {"-pullfile" })
    private boolean pull=false;

    @Option(name = "-pullmode", usage = "Pull Mode: WATCH/ONCE", depends = {"-pull"} )
    private PULLMODE pmode=PULLMODE.ONCE;

    @Option(name = "-pullfile", usage = "Set Which Config File should be pulled", depends = {"-pull"})
    private String pullFile;

    @Option(name = "-push", usage = "Set the Push Mode",forbids = {"-pull"},depends = {"-pushfile"})
    private boolean push=false;

    @Option(name = "-pushfile", usage = "File to be pushed. ", depends = {"-push"})
    private File pushFile;

    @Option(name = "-o", usage = "The place where file pulled to be saved", depends = {"-pull"})
    private String objectFilePath;

    @Option(name = "-v", usage = "Print file updater version",required = false)
    private boolean isVersion;

    @Option(name = "-h", usage = "Print Help Information",required = false)
    private boolean isHelp;

    // receives other command line parameters than options
    @Argument
    private List<String> arguments = new ArrayList<String>();

    public static void main(String[] args) {

        MainApp app = null;
        //Push Main Process
        try {
            app = new MainApp();
            app.parseCmdLine(args);
            if (app.isVersion){
                System.out.println("Hadoop File Updater, Version:" + CONSTANTSUTIL.VERSION + ", Copyright CMBC, Author : Huang Pengcheng");
                return;
            }

            app.getZKClient();
            if (app.pull){
                Puller puller = new Puller(app.client,app.fileType,app.pullFile,app.objectFilePath,app.pmode);

                puller.pullFromZK();

            }

            if (app.push){
                Pusher pusher = new Pusher(app.client,app.fileType,app.pushFile);
                pusher.pushToZK();
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (app !=null && app.client!= null)
                app.client.close();
        }
    }

    private void getZKClient() {
        this.client = ZKUtils.INSTANCE.getClient();
    }

    private void parseCmdLine(String args[]) {
        CmdLineParser parser = new CmdLineParser(MainApp.this);
        try {
            parser.parseArgument(args);

            if(!isHelp && !isVersion && !pull && !push) {
                throw new CmdLineException(parser,"Error:One of the Pull or Push mode must be selected.", new Throwable());
            }

            if(isHelp){
                throw new CmdLineException(parser,"", new Throwable());
            }

            if (fileType==FILETYPE.UNKNOWN){
                throw new CmdLineException(parser,"Error: File type (-t) must be specified", new Throwable());
            }


        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            System.err.println("hadoopconfigupdater [options...] arguments...");
            // print the list of available options
            parser.printUsage(System.err);
            System.err.println();

            System.exit(-1);
        }
    }
        public CuratorFramework getClient() {
        return client;
    }

}
