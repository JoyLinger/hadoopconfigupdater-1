import cmbc.bigdata.core.Puller;
import cmbc.bigdata.core.Pusher;
import cmbc.bigdata.utils.CommandLineValues;
import cmbc.bigdata.utils.ZKUtils;
import org.apache.curator.framework.CuratorFramework;
import org.kohsuke.args4j.CmdLineParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by huangpengcheng on 2016/7/21 0021.
 */
public class MainAppTest {

    private static final Logger logger = LoggerFactory.getLogger(MainAppTest.class);
    private static final boolean DISABLEPUSH = false;
    private static CuratorFramework client;

    public static void main(String[] args) {
        args = testPushFile();
        //Push Main Process
        try {
            CommandLineValues cm = new CommandLineValues(args);
            cm.setDisablePushMode(DISABLEPUSH);
            cm.parseCmd();
            if (cm.isVersion()) return;

            getZKClient(cm.getZkStr());
            if (cm.isPull()){
                Puller puller = new Puller(client,cm.getFileType(),cm.getPullFiles(),
                        cm.getPullMode(),cm.getCallBack(),cm.getChangeMode());
                puller.pullFromZK();
            }

            if (cm.isPush()){
                Pusher pusher = new Pusher(client,cm.getFileType(),cm.getPushFiles());
                pusher.pushToZK();
            }

        } catch (Exception e) {
            logger.error("Exception:",e);
        } finally {
            if (client!= null)
                client.close();
        }
    }

    private static void getZKClient(String conn) {
        ZKUtils.INSTANCE.setZk(conn);
        client =ZKUtils.INSTANCE.getClient();
    }

    private static void parseCmdLine(String args[]) {
        CmdLineParser parser = new CmdLineParser(MainAppTest.class);

    }

    public CuratorFramework getClient() {
        return client;
    }

    /**
     * 1. push file.
     *
     */
    private static String[] testPushFile(){

        String[] args = new String[5];
        args[0] = "-push";
        args[1] = "-pushfiles";
        args[2] = "/root/hosts";
        args[3] = "-zk";
        args[4] = "192.168.188.2:2181";
        return args;
    }

    /**
     * 2. pull file.
     * @param pmode pull mode.
     * @param cmode change mode.
     */
    private static String[] testPullFile(String pmode, String cmode){
        String[] args = new String[9];
        args[0] = "-pull";
        args[1] = "-pullfiles";
        args[2] = "/etc/hosts";
        args[3] = "-zk";
        args[4] = "192.168.188.2:2181";
        args[5] = "-pullmode";
        args[6] = pmode;
        args[7] = "-c";
        args[8] = cmode;
        return args;
    }


}
