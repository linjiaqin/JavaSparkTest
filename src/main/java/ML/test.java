package ML;

public class test {
    public static class A{
        double[] s;

        public A(double[] s) {
            this.s = s;
        }

        public double[] getS() {
            return s;
        }

        public void setS(double[] s) {
            this.s = s;
        }
    }

    public static void main(String[] args) {
        //就是说A里面的s只是指向了sss，如果sss改变，s就会改变
        double[] sss = new double[]{1.0,2.0,3.0};
        A a = new A(sss);
        double[] tmp = a.getS();
        for (int i = 0; i < tmp.length; i++) {
            System.out.println(tmp[i]);
        }

        sss[2] = 100.0;
        double[] tmp2 = a.getS();
        for (int i = 0; i < tmp2.length; i++) {
            System.out.println(tmp2[i]);
        }
    }
}
