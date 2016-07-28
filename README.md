Use Zookeeper to distribute hadoop config file (Any file less than 1MB, actually)
    hadoopconfigupdater [options...] arguments...
    -h                         : Print Help Information (default: false)
    -o VAL                     : The dir where file pulled to be saved
    -pull                      : Set the Pull Mode (default: false)
    -pullfile VAL              : Set Which Config File should be pulled, can be a list: hdfs-site.xml,core-site.xml
    -pullmode [WATCH | ONCE]   : Pull Mode: WATCH/ONCE (default: ONCE)
    -push                      : Set the Push Mode (default: false)
    -pushfile FILE             : File to be pushed.
    -t [PLAIN | XML | UNKNOWN] : File type: HOSTS/XML/UNKNOWN. (default: PLAIN)
    -v                         : Print file updater version (default: false)
    -zk VAL                    : Zookeeper Addresses, e.g, 127.0.0.1:2181,127.0.0.2:2181
