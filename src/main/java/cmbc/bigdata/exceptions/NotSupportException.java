package cmbc.bigdata.exceptions;

/**
 * Created by huangpengcheng on 2016/9/29 0029.
 */
public class NotSupportException extends Exception{
    public NotSupportException(String msg) {
        super("Not Supported Exception! " + msg);
    }
}
