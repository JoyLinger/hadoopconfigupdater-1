package cmbc.bigdata.core;

import cmbc.bigdata.constants.CONSTANTSUTIL;
import cmbc.bigdata.constants.FILETYPE;
import cmbc.bigdata.utils.WholeFileHandler;
import cmbc.bigdata.utils.XMLHandler;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by huangpengcheng on 2016/7/20 0020.
 */
public class Pusher {
    private static final Logger logger = LoggerFactory.getLogger(Pusher.class);

    private final CuratorFramework client;
    private final FILETYPE fileType;
    private final String[] pushFiles;

    public Pusher(CuratorFramework client, FILETYPE fileType, String pushFiles) {
        this.client = client;
        this.fileType = fileType;
        this.pushFiles = pushFiles.split(",");
    }

    public void pushToZK() throws Exception {
        logger.error("Push mode selected.");
        System.out.println();
        for(String pushFilePath : pushFiles){
            File pushFile = new File(pushFilePath);
            String parentPath= "/" +  pushFilePath.substring(pushFilePath.lastIndexOf('/') + 1);

            if(!pushFile.exists()){
                logger.error("Error:" + pushFilePath + "doesn't exist, Skip it and push next File:");
            }

            if (fileType == FILETYPE.PLAIN) {
                WholeFileHandler fileHandler = new WholeFileHandler(pushFile);
                if (client.checkExists().forPath(parentPath) == null) {
                    client.create().forPath(parentPath);
                }
                client.setData().forPath(parentPath,fileHandler.fileToBytes());
                logger.info("Succeed!" + pushFile.getCanonicalPath() + " has been pushed to /" + CONSTANTSUTIL.DEFAULT_NS + parentPath );
            }
            else if (fileType == FILETYPE.XML) {
                HashMap<String, String> kvMap = new HashMap<String, String>();
                XMLHandler xmlHandler = new XMLHandler(pushFile);
                kvMap = xmlHandler.parseConfXML();
                //If parent path doesn't exist,Create it
                //Clear the children of the path
                if (client.checkExists().forPath(parentPath) != null) {
                    client.delete().deletingChildrenIfNeeded().forPath( "/"  + pushFile.getName());
                }

                //Create the Path
                client.create().forPath(parentPath, parentPath.getBytes());

                //Push the KV into zk
                for (Map.Entry<String, String> entry : kvMap.entrySet()) {
                    String key =  "/"  + pushFile.getName() +  "/"  + entry.getKey();
                    byte[] value = entry.getValue().getBytes();
                    if (client.checkExists().forPath(key) == null) {
                        // Create for Non-Exist node
                        client.create().forPath(key, value);
                        logger.info("Create Path:" + key + "\t Value:" + new String(value));
                    } else {
                        client.setData().forPath(key, value);
                        logger.info("Update Path:" + key + "\t Value:" + new String(value));
                    }
                }
            }
            else
                logger.error("Unknown type. Aborted and Exiting...");
        }
    }
}
