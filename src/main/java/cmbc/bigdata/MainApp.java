package cmbc.bigdata;

import cmbc.bigdata.constants.CONSTANTSUTIL;
import cmbc.bigdata.constants.FILETYPE;
import cmbc.bigdata.constants.PULLMODE;
import cmbc.bigdata.core.Puller;
import cmbc.bigdata.core.Pusher;
import cmbc.bigdata.utils.ZKUtils;
import org.apache.curator.framework.CuratorFramework;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by huangpengcheng on 2016/7/21 0021.
 */
public class MainApp {

    private static final Logger logger = LoggerFactory.getLogger(MainApp.class);

    private CuratorFramework client;

    @Option(name = "-t", usage = "File type: HOSTS/XML/UNKNOWN.")
    private FILETYPE fileType=FILETYPE.PLAIN;

    @Option(name = "-pullmode", usage = "Pull Mode: WATCH/ONCE", depends = {"-pull"} )
    private PULLMODE pmode=PULLMODE.ONCE;

    @Option(name = "-pull", usage = "Set the Pull Mode",forbids = {"-push"},depends = {"-pullfiles" })
    private boolean pull=false;

    @Option(name = "-pullfiles", usage = "Set which files should be pull down(depends on the name) and where pulled Files should be saved, " +
            "can be a list: /etc/hadoop/hdfs-site.xml,./etc/core-site.xml " +
            "will pull hdfs-site.xml and core-site.xml, and then saved to the location specified",
            depends = {"-pull"})
    private String pullFiles;

    @Option(name = "-push", usage = "Set the Push Mode",forbids = {"-pull"},depends = {"-pushfiles"})
    private boolean push=false;

    @Option(name = "-pushfiles", usage = "Files to be pushed." +
            "Both Relative and absolute path are supported .A comma as a separator." +
            "e.g. /etc/hosts,/etc/services,./stub.sh,makejar.sh  ",
            depends = {"-push"})
    private String pushFiles;

    @Option(name = "-v", usage = "Print file updater version",required = false)
    private boolean isVersion;

    @Option(name = "-h", usage = "Print Help Information",required = false)
    private boolean isHelp;

    @Option(name = "-zk" , usage = "Zookeeper Addresses List, e.g, 127.0.0.1:2181,127.0.0.2:2181")
    private String zkStr="127.0.0.1:2181";

    @Option(name = "-callBack" , usage = "Callback command executed when pull in watch mode, " +
            "filename(absolute path) will be passed as a parameter", depends = {"-pull"})
    private String callBack;

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
                return;
            }

            app.getZKClient(app.zkStr);
            if (app.pull){
                Puller puller = new Puller(app.client,app.fileType,app.pullFiles,app.pmode,app.callBack);
                puller.pullFromZK();
            }

            if (app.push){
                Pusher pusher = new Pusher(app.client,app.fileType,app.pushFiles);
                pusher.pushToZK();
            }

        } catch (Exception e) {
            logger.error("Exception:",e);
        } finally {
            if (app !=null && app.client!= null)
                app.client.close();
        }
    }

    private void getZKClient(String conn) {
        ZKUtils.INSTANCE.setZk(conn);
        this.client =ZKUtils.INSTANCE.getClient();
    }

    private void parseCmdLine(String args[]) {
        CmdLineParser parser = new CmdLineParser(MainApp.this);
        try {
            parser.parseArgument(args);

            //Pull or Push mode must be specified.
            if(!isHelp && !isVersion && !pull && !push) {
                throw new CmdLineException(parser,"Error:One of the Pull or Push mode must be selected.", new Throwable());
            }

            //ZK addresses must be set
            if (!isHelp && !isVersion && zkStr.equals("127.0.0.1:2181")){
                logger.warn("Default ZK Address(127.0.0.1:2181) is used");
            }

            // If help is needed
            if(isHelp){
                throw new CmdLineException(parser,"", new Throwable());
            }

            //File type must be set
            if (fileType==FILETYPE.UNKNOWN){
                throw new CmdLineException(parser,"Error: File type (-t) must be specified", new Throwable());
            }

        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            System.out.println("Hadoop File Updater, Version:" + CONSTANTSUTIL.VERSION + ", Copyright CMBC, Author : huangpengcheng@cmbc.com.cn");
            System.err.println("hadoopconfigupdater [options...] arguments...");
            // print the list of available options
            parser.printUsage(System.err);
            System.err.println();
            System.exit(-1);
        }
        finally {
            System.out.println("Hadoop File Updater, Version:" + CONSTANTSUTIL.VERSION + ", Copyright CMBC, Author : huangpengcheng@cmbc.com.cn");
        }
    }
        public CuratorFramework getClient() {
        return client;
    }

}
