package distinct.HyperLogLog;

import distinct.HyperLogLog.suanfa.HyperLogLog;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

public class HyperLogLogTest {
    public static void main(String[] args) throws Exception {
        String filePath = "000000_0";
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(filePath)));

        Set<String> values =new HashSet<>();
        HyperLogLog logLog=new HyperLogLog(0.01); //允许误差

        String line = "";
        while ((line = br.readLine()) != null) {
            String[] s = line.split(",");
            String uuid = s[0];
            values.add(uuid);
            logLog.offer(uuid);
        }

        long rs=logLog.cardinality();
    }
}
