package remove_duplicate;

import java.io.File;

/**
 * Copyright (C), 2015-2018, XXX有限公司
 * FileName: word_count.FileUtil
 * Author:   lvshuo
 * Date:     27/03/2018 2:33 PM
 * Description: File utility to handle folder
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */

/**
 * 〈一句话功能简述〉<br> 
 * 〈File utility to handle folder〉
 *
 * @author lvshuo
 * @create 27/03/2018
 * @since 1.0.0
 */
public class FileUtil {

    public static boolean deleteDir(String path) {
        File dir = new File(path);
        if (dir.exists()) {
            for (File f : dir.listFiles()) {
                if (f.isDirectory()) {
                    deleteDir(f.getName());
                } else {
                    f.delete();
                }
            }
            dir.delete();
            return true;
        } else {
            System.out.println("文件(夹)不存在!");
            return false;
        }
    }

}