 
public class MyThread
{
    public static void main(String[] args)
    {
        Reservoir r = new Reservoir(100);
        Booth b1 = new Booth(r);
        Booth b2 = new Booth(r);
        Booth b3 = new Booth(r);
    }
}

class Reservoir {
    private int total;

    public Reservoir(int t)
    {
        this.total = t;
    }

    public synchronized boolean sellTicket()
    {
        if(this.total > 0) {
            this.total = this.total - 1;
            return true;
        }
        else {
            return false;
        }
    }
}

class Booth extends Thread {
    private static int threadID = 0;

    private Reservoir release;
    private int count = 0;

    public Booth(Reservoir r) {
        super("ID:"+(++threadID));
        this.release = r;
        this.start();
    }

    public String toString() {
        return super.getName();
    }

    public void run() {
        while(true) {
            if(this.release.sellTicket()) {
                this.count = this.count +1;
                System.out.println(this.getName() + ":sell 1");
                try {
                    sleep((int) Math.random()*100);
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            else {
                break;
            }
        }
        System.out.println(this.getName() +" sold:" + count);
    }
}
