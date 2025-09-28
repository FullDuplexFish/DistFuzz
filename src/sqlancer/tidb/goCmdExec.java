package sqlancer.tidb;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.Runtime;

import java.util.*;

public class goCmdExec {
     public String executeLinuxCmd(List<String> cmd) {
        String line = null;
        String res = null;
        try{
            ProcessBuilder pb = new ProcessBuilder(cmd);
            pb.redirectErrorStream(true); // 合并 stdout 和 stderr
            Process proc = pb.start();    
                    // 读取输出
            
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream()))) {
                while ((line = reader.readLine()) != null) {
                    //System.out.println("get return:" + line);
                    if(line != null) {
                        res = line;
                    }
                }
            }
            int retCode =  proc.waitFor();


        }catch(Exception e){
            e.printStackTrace();
        }
        return res;

    }

    
}
