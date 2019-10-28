package com.ytfs.service.check;

import com.ytfs.common.SerializationUtil;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public class MessageWriter {

    private static final List objlist = new ArrayList();

    public static void write(Object obj) {
        if (!objlist.contains(obj.getClass().getSimpleName())) {
            byte[] bs = SerializationUtil.serialize(obj);
            String path = "/" + obj.getClass().getSimpleName() + ".dat";
            OutputStream os = null;
            try {
                os = new FileOutputStream(path);
                os.write(bs);
            } catch (IOException ex) {
            } finally {
                if (os != null) {
                    try {
                        os.close();
                    } catch (IOException ex) {

                    }
                }
            }
            objlist.add(obj.getClass().getSimpleName());
        }
    }
}
