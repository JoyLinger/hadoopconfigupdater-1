package cmbc.bigdata;

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
public class MainApp {

    private static final Logger logger = LoggerFactory.getLogger(MainApp.class);
    private static final boolean DISABLEPUSH = true;
    private static CuratorFramework client;

    public static void main(String[] args) {

        //Push Main Process
        try {
            CommandLineValues cm = new CommandLineValues(args);
            cm.setDisablePushMode(DISABLEPUSH);
            cm.parseCmd();
            if (cm.isVersion()){
                return;
            }

            getZKClient(cm.getZkStr());
            if (cm.isPull()){
                Puller puller = new Puller(client,cm.getFileType(),cm.getPullFiles(),
                        cm.getPullMode(),cm.getCallBack());
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
        CmdLineParser parser = new CmdLineParser(MainApp.class);

    }

    public CuratorFramework getClient() {
        return client;
    }

}
