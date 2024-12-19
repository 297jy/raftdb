package org.zhuanyi.jraftdb.engine.log;

import org.zhuanyi.jraftdb.engine.dto.Status;
import org.zhuanyi.jraftdb.engine.utils.Slice;


public interface WritableFile {

    /**
     * 追加数据到缓冲区
     * @param data
     * @return
     */
    Status append(Slice data);

    /**
     * 关闭并刷新缓冲区
     * @return
     */
    Status close();

    /**
     * 刷新缓冲区
     * @return
     */
    Status flush();

    Status sync();

}
