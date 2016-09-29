package cmbc.bigdata.utils;

import cmbc.bigdata.constants.CONSTANTSUTIL;
import cmbc.bigdata.constants.FILETYPE;
import cmbc.bigdata.constants.PULLMODE;
import cmbc.bigdata.exceptions.NotSupportException;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by huangpengcheng on 2016/9/29 0029.
 */
public class CommandLineValues {
    private static final Logger logger = LoggerFactory.getLogger(CommandLineValues.class);
    private CmdLineParser parser;
    private boolean disablePushMode;
    private String[] arguments;

    @Option(name = "-t", usage = "File type: HOSTS/XML/UNKNOWN.")
    private FILETYPE fileType = FILETYPE.PLAIN;

    @Option(name = "-pullmode", usage = "Pull Mode: WATCH/ONCE", depends = {"-pull"})
    private PULLMODE pmode = PULLMODE.ONCE;

    @Option(name = "-pull", usage = "Set the Pull Mode", forbids = {"-push"}, depends = {"-pullfiles"})
    private boolean pull = false;

    @Option(name = "-pullfiles", usage = "Set which files should be pull down(depends on the name) and where pulled Files should be saved, " +
            "can be a list: /etc/hadoop/hdfs-site.xml,./etc/core-site.xml " +
            "will pull hdfs-site.xml and core-site.xml, and then saved to the location specified",
            depends = {"-pull"})
    private String pullFiles;

    @Option(name = "-push", usage = "Set the Push Mode", forbids = {"-pull"}, depends = {"-pushfiles"})
    private boolean push = false;

    @Option(name = "-pushfiles", usage = "Files to be pushed." +
            "Both Relative and absolute path are supported .A comma as a separator." +
            "e.g. /etc/hosts,/etc/services,./stub.sh,makejar.sh  ",
            depends = {"-push"})
    private String pushFiles;

    @Option(name = "-v", usage = "Print file updater version", required = false)
    private boolean isVersion;

    @Option(name = "-h", usage = "Print Help Information", required = false)
    private boolean isHelp;

    @Option(name = "-zk", usage = "Zookeeper Addresses List, e.g, 127.0.0.1:2181,127.0.0.2:2181")
    private String zkStr = "127.0.0.1:2181";

    @Option(name = "-callBack", usage = "Callback command executed when pull in watch mode, " +
            "filename(absolute path) will be passed as a parameter", depends = {"-pull"})
    private String callBack;


    public CommandLineValues(String... args) {
        parser = new CmdLineParser(this);
        arguments = args;
    }

    public void parseCmd() {

        try {
            parser.parseArgument(arguments);

            if (isPush() && disablePushMode) {
                throw new NotSupportException("Error: Push mode has been banned in this version");
            }

            //Pull or Push mode must be specified.
            if (!isHelp() && !isVersion() && !isPull() && !isPush()) {
                throw new CmdLineException(parser, "Error:One of the Pull or Push mode must be selected.", new Throwable());
            }

            //ZK addresses must be set
            if (!isHelp() && !isVersion() && getZkStr().equals("127.0.0.1:2181")) {
                logger.warn("Default ZK Address(127.0.0.1:2181) is used");
            }

            // If help is needed
            if (isHelp()) {
                throw new CmdLineException(parser, "", new Throwable());
            }

            //File type must be set
            if (getFileType() == FILETYPE.UNKNOWN) {
                throw new CmdLineException(parser, "Error: File type (-t) must be specified", new Throwable());
            }

        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            System.out.println("Hadoop File Updater, Version:" + CONSTANTSUTIL.VERSION + ", Copyright @CMBC, Author : huangpengcheng@cmbc.com.cn");
            System.err.println("hadoopconfigupdater [options...] arguments...");
            // print the list of available options
            parser.printUsage(System.err);
            System.err.println();
            System.exit(-1);
        } catch (NotSupportException e) {
            System.out.println("Hadoop File Updater, Version:" + CONSTANTSUTIL.VERSION + ", Copyright @CMBC, Author : huangpengcheng@cmbc.com.cn");
            System.err.println(e.getMessage());
            System.exit(-1);
        }
        System.out.println("Hadoop File Updater, Version:" + CONSTANTSUTIL.VERSION + ", Copyright @CMBC, Author : huangpengcheng@cmbc.com.cn");
    }

    public void setDisablePushMode(boolean disablePushMode) {
        this.disablePushMode = disablePushMode;
    }

    public FILETYPE getFileType() {
        return fileType;
    }

    public PULLMODE getPullMode() {
        return pmode;
    }

    public boolean isPull() {
        return pull;
    }

    public String getPullFiles() {
        return pullFiles;
    }

    public boolean isPush() {
        return push;
    }

    public String getPushFiles() {
        return pushFiles;
    }

    public boolean isVersion() {
        return isVersion;
    }

    public boolean isHelp() {
        return isHelp;
    }

    public String getZkStr() {
        return zkStr;
    }

    public String getCallBack() {
        return callBack;
    }
}
