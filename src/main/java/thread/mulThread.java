package thread;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class submitThread implements Runnable{

    private String str;
    public submitThread(String sttr){
        this.str = str;
    }
    @Override
    public void run() {
        try {
            Process process = Runtime.getRuntime().exec(str);
            InputStream stdin = process.getInputStream();
            InputStreamReader isr = new InputStreamReader(stdin);
            BufferedReader br = new BufferedReader(isr);
            String line = "";
            StringBuilder res = new StringBuilder("");
            while ((line = br.readLine()) != null) {
                res.append(line);
            }
            int exitVal = process.waitFor();
            System.out.println(exitVal);
            System.out.println(res);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
public class mulThread {
    public static void main(String[] args) throws IOException, InterruptedException {
        String str = "ls /home/linjiaqin";
        //ExecutorService executorService = Executors.newCachedThreadPool();
       // executorService.submit(new submitThread(commond));
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Process process = Runtime.getRuntime().exec(str);
        InputStream stdin = process.getInputStream();
        InputStreamReader isr = new InputStreamReader(stdin);
        BufferedReader br = new BufferedReader(isr);
        String line = "";
        StringBuilder res = new StringBuilder("");
        while ((line = br.readLine()) != null) {
            res.append(line);
        }
//        int exitVal = process.waitFor();
//        System.out.println(exitVal);
        System.out.println(res);

    }
}
