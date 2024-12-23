package org.zhuanyi.jraftdb.engine.utils;

import org.zhuanyi.jraftdb.engine.dto.Status;
import org.zhuanyi.jraftdb.engine.utils.slice.Slice;


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

    /**
     * 将数据同步写入文件并落盘，因为数据即使被write，也只是保存在pagecache中，存在数据丢失的风险
     * @return
     */
    Status sync();

}
