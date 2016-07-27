package cmbc.bigdata.utils;

import cmbc.bigdata.constants.CONSTANTSUTIL;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.server.quorum.Follower;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.SAXReader;
import org.dom4j.io.XMLWriter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;

/**
 * Created by huangpengcheng on 2016/7/23 0023.
 */
public class XMLHandler {

    private String filePath;

    public Document getDocument() {
        return document;
    }

    private Document document;

    public XMLHandler(String filePath) {
        this.filePath = filePath;
        initDocument(this.filePath);
    }

    public XMLHandler(File xmlFile){
        try {
            this.filePath = xmlFile.getCanonicalPath();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Parse Hadoop Config file in XML (push mode)
     *
     * @return
     * @throws DocumentException
     * @throws IOException
     */
    public HashMap<String, String> parseConfXML() {
        HashMap<String, String> propMap = new HashMap<String, String>();
        List<Element> propList = getProps();
        for (Element prop : propList) {
            propMap.put(
                    prop.element("name").getStringValue(),
                    (
                            prop.element("value").getStringValue()+
                                    (prop.element("description")!=null?
                                            (CONSTANTSUTIL.VALUE_DESC_SPLIT+ prop.element("description").getStringValue()):""
                                    )
                    )
            );
        }
        return propMap;
    }

    public boolean checkPropExistInXML(String propName) {
        boolean exist = false;
        List<Element> propList = getProps();
        for (Element prop : propList) {
            if (prop.element("name").getStringValue().equals(propName)) {
                exist = true;
                break;
            }
        }

        return exist;
    }

    public void updateOrCreatePropInXML(String propName, String propValue) {
        String[] val_des = propValue.split(CONSTANTSUTIL.VALUE_DESC_SPLIT);
        List<Element> propList = getProps();
        //Update
        for (Element prop : propList) {
            if (prop.element("name").getStringValue().equals(propName)) {
                //Exist
                prop.element("value").setText(val_des[0]);
                if (val_des.length==2) {
                    //Judge Description exist
                    if(prop.element("description")!=null){
                        prop.element("description").elementText(val_des[1]);
                    }
                    else{
                        prop.addElement("description").addText(val_des[1]);
                    }
                }
                return;
            }
        }

        //Create
        createPropInXML(propName,propValue);
    }


    /**
     * FilePath -> List<Element> Function
     *
     * @return
     * @throws DocumentException
     */
    private void initDocument(String filePath) {
        SAXReader reader = new SAXReader();
        File file = new File(filePath);
        convertDoc(filePath, reader, file);
    }


    private void initDocument(File xmlFile) {
        SAXReader reader = new SAXReader();
        convertDoc(filePath, reader, xmlFile);
    }

    private void convertDoc(String filePath, SAXReader reader, File file) {
        if (file.exists()) {
            try {
                document = reader.read(filePath);
            } catch (DocumentException e) {
                e.printStackTrace();
                System.exit(-1);
            }
        } else {
            document = DocumentHelper.createDocument();
        }
    }

    public void createPropInXML(String propName, String propValue) {
        String[] val_des = propValue.split(CONSTANTSUTIL.VALUE_DESC_SPLIT);
        //Create
        Element root = document.getRootElement();
        Element propertyElement =root.addElement("property");
        propertyElement.addElement("name").addText(propName);
        propertyElement.addElement("value").addText(val_des[0]);
        if (val_des.length==2) {
            propertyElement.addElement("description").addText(val_des[1]);
        }
    }

    public void deletePropInXML(String propName){
        Element root = document.getRootElement();
        List<Element>  elementList = getProps();
        for(Element prop:elementList){
            if(prop.element("name").getStringValue().equals(propName)){
               root.remove(prop);
            }
        }
    }

    private List getProps() {
        Element root = document.getRootElement();
        return root.elements("property");
    }

    public void writeToXML() throws IOException {
        OutputFormat format = OutputFormat.createPrettyPrint();
        //Add Empty Element like <e></e>
        format.setExpandEmptyElements(true);
        XMLWriter writer = new XMLWriter(new FileWriter(filePath), format);
        writer.write(document);
        writer.close();
//        FileWriter out = new FileWriter(filePath);
//        document.write(out);
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

}
