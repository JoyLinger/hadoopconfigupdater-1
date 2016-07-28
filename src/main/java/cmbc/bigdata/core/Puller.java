package cmbc.bigdata.core;

import cmbc.bigdata.constants.CONSTANTSUTIL;
import cmbc.bigdata.constants.FILETYPE;
import cmbc.bigdata.constants.PULLMODE;
import cmbc.bigdata.utils.XMLHandler;
import org.apache.commons.io.FileUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.*;
import org.dom4j.DocumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.UnknownFormatFlagsException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by huangpengcheng on 2016/7/20 0020.
 */
public class Puller {
    private static final Logger logger = LoggerFactory.getLogger(Puller.class);
    private final static ExecutorService EXECUTOR_SERVICE = Executors.newSingleThreadExecutor();
    private final CuratorFramework client;
    private final FILETYPE fileType;
    private final String pullFileName;
    private final String objectFileName;
    private final PULLMODE pmode;
    private XMLHandler xmlHandler;

    public Puller(CuratorFramework client, FILETYPE fileType, String pullFileName, String objectFilePath, PULLMODE pmode) throws DocumentException {
        this.client = client;
        this.fileType = fileType;
        this.pullFileName = pullFileName;
        this.objectFileName= objectFilePath;
        this.pmode = pmode;
        if (fileType==FILETYPE.XML)
            this.xmlHandler = new XMLHandler(pullFileName);
    }

    /**
     * Watch One Node Change
     *
     * @throws Exception
     */
    public void watchDataChanged() throws Exception {
        String[]  filesPath = this.pullFileName.split(",");
        if (filesPath.length == 0) {
            throw new UnknownFormatFlagsException("Error: Wrong Watch File List format");
        }
        for( final String file : filesPath){
            final NodeCache nodeCache = new NodeCache(client,  "/"  + file);
            nodeCache.getListenable().addListener(
                    new NodeCacheListener() {
                        public void nodeChanged() throws Exception {
                            ChildData data = nodeCache.getCurrentData();
                            if (null != data) {
                                logger.info("node data changed, new data:" + new String(nodeCache.getCurrentData().getData()));
                                FileUtils.writeStringToFile(new File(objectFileName+ "/" +file),new String(nodeCache.getCurrentData().getData(),"UTF-8"));
                            }
                        }
                    }, EXECUTOR_SERVICE);
            nodeCache.start(true);
        }

    }

    /**
     * Watch ths Child node of a path
     *
     * @throws Exception
     */
    public void watchChildrenChanged() throws Exception {
        PathChildrenCache cache = new PathChildrenCache(client,  "/"  + this.pullFileName, true);
        cache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
        cache.getListenable().addListener(new PathChildrenCacheListener() {

            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                String configPath = event.getData().getPath();
                String configName = configPath.substring(configPath.lastIndexOf( "/" ) + 1);
                String configContent = new String(event.getData().getData());
                if (fileType == FILETYPE.XML) {
                    switch (event.getType()) {
                        case CHILD_ADDED:
                        case CHILD_UPDATED:
                            logger.info("Add/Update the config:" + configPath);
                            xmlHandler.updateOrCreatePropInXML(configName, configContent);
                            xmlHandler.writeToXML();
                            printEventInfo(event);
                            break;
                        case CHILD_REMOVED:
                            logger.info("Remove the config:" + configPath);
                            xmlHandler.deletePropInXML(configName);
                            xmlHandler.writeToXML();
                            printEventInfo(event);
                            break;
                        case CONNECTION_SUSPENDED:
                            break;
                        case CONNECTION_RECONNECTED:
                            break;
                        case CONNECTION_LOST:
                            break;
                        case INITIALIZED:
                            break;
                        default:
                            printEventInfo(event);
                            break;
                    }
                }

                if (fileType == FILETYPE.PLAIN) {
                    switch (event.getType()) {
                        case CHILD_ADDED:
                        case CHILD_UPDATED:
                            logger.info("Add/Update the config:" + configPath);
                            printEventInfo(event);
                            break;
                        case CHILD_REMOVED:
                            logger.info("Remove the config:" + configPath);
                            printEventInfo(event);
                            break;
                        case CONNECTION_SUSPENDED:
                            break;
                        case CONNECTION_RECONNECTED:
                            break;
                        case CONNECTION_LOST:
                            break;
                        case INITIALIZED:
                            break;
                        default:
                            printEventInfo(event);
                            break;
                    }
                }
            }

            private void printEventInfo(PathChildrenCacheEvent event) {
                logger.info("======================================");
                logger.info("Event Type:" + event.getType().toString());
                logger.info("Event Path:" + event.getData().getPath());
                logger.info("Event Content:" + new String(event.getData().getData()));
                logger.info("Event Data Version:" + String.valueOf(event.getData().getStat().getVersion()) + "\n");
            }

        });

    }

    public void pullFromZK() throws Exception {
        if (fileType == FILETYPE.XML) {
            if(pmode == PULLMODE.ONCE) {
                List<String> childNodes = client.getChildren().forPath("/" + pullFileName);
                StringBuilder keyBuilder = new StringBuilder();
                String value;
                if (!(new File(pullFileName)).exists()) {
                    //File nonexist, only creat e element
                    xmlHandler.getDocument().addElement("configuration");
                    for (String child : childNodes) {
                        logger.info("Pull config: " + child);
                        keyBuilder.append( "/" ).append(pullFileName).append( "/" ).append(child);
                        value = new String(client.getData().forPath(keyBuilder.toString()));
                        xmlHandler.createPropInXML(child, value);
                        logger.info("Create Config name: " + child +
                                " ,Config value:" + value.replace(CONSTANTSUTIL.VALUE_DESC_SPLIT, ", Config description:"));
                        keyBuilder.setLength(0);
                    }
                } else {  //File exist, update or create element
                    for (String child : childNodes) {
                        logger.info("Pull config: " + child);
                        keyBuilder.append( "/" ).append(pullFileName).append( "/" ).append(child);
                        value = new String(client.getData().forPath(keyBuilder.toString()));
                        xmlHandler.updateOrCreatePropInXML(child, value);
                        keyBuilder.setLength(0);
                        logger.info("Update Config name: " + child +
                                " ,Config value:" + value.replace(CONSTANTSUTIL.VALUE_DESC_SPLIT, ", Config description:"));
                    }
                }
                xmlHandler.writeToXML();
            }
            else if(pmode == PULLMODE.WATCH){
                watchChildrenChanged();
            }
            else{
                throw new UnsupportedOperationException();
            }
        }

        if (fileType == FILETYPE.PLAIN) {
            if(pmode == PULLMODE.ONCE){
                String content = new String(client.getData().forPath( "/" + pullFileName));
                FileUtils.writeStringToFile(new File(objectFileName+"/"+pullFileName),content,"UTF-8");
                logger.info("Succeed! File "+ pullFileName + " has been pulled to " + objectFileName);
            }
            else if(pmode == PULLMODE.WATCH){
                CountDownLatch latch = new CountDownLatch(1);
                watchDataChanged();
                latch.await();
            }
            else{
                throw new UnsupportedOperationException();
            }
        }
    }
}
