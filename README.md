Use Zookeeper to distribute hadoop config file (Any file less than 1MB, actually)

    Hadoop File Updater, Version:1.1, Copyright CMBC, Author : huangpengcheng@cmbc.com.cn
    hadoopconfigupdater [options...] arguments...
    
       -c [OVERWRITE | APPEND]    : Change Mode: OVERWRITE/APPEND (default: OVERWRITE)
       -callBack VAL              : Callback command executed when pull in watch
                                    mode, filename(absolute path) will be passed as a
                                    parameter
       -h                         : Print Help Information (default: true)
       -pull                      : Set the Pull Mode (default: false)
       -pullfiles VAL             : Set which files should be pull down(depends on
                                    the name) and where pulled Files should be saved,
                                    can be a list: /etc/hadoop/hdfs-site.xml,./etc/cor
                                    e-site.xml will pull hdfs-site.xml and
                                    core-site.xml, and then saved to the location
                                    specified
       -pullmode [WATCH | ONCE]   : Pull Mode: WATCH/ONCE (default: ONCE)
       -push                      : Set the Push Mode (default: false)
       -pushfiles VAL             : Files to be pushed.Both Relative and absolute
                                    path are supported .A comma as a separator.e.g.
                                    /etc/hosts,/etc/services,./stub.sh,makejar.sh
       -t [PLAIN | XML | UNKNOWN] : File type: HOSTS/XML/UNKNOWN. (default: PLAIN)
       -v                         : Print file updater version (default: false)
       -zk VAL                    : Zookeeper Addresses List, e.g, 127.0.0.1:2181,127.
                                    0.0.2:2181 (default: 127.0.0.1:2181) 
