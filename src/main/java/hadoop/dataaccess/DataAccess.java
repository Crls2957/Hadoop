package hadoop.dataaccess;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DataAccess implements Writable {

    private String num;
    private long up;
    private long down;
    private long sum;

    public DataAccess(){

    }
    public DataAccess(String num,long up,long down){
        this.num=num;
        this.up=up;
        this.down=down;
        this.sum=up+down;
    }

    public String getNum() {
        return num;
    }

    public void setNum(String num) {
        this.num = num;
    }

    public long getUp() {
        return up;
    }

    public void setUp(long up) {
        this.up = up;
    }

    public long getDown() {
        return down;
    }

    public void setDown(long down) {
        this.down = down;
    }

    public long getSum() {
        return sum;
    }

    public void setSum(long sum) {
        this.sum = sum;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(num);
        dataOutput.writeLong(up);
        dataOutput.writeLong(down);
        dataOutput.writeLong(sum);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.num=dataInput.readUTF();
        this.up=dataInput.readLong();
        this.down=dataInput.readLong();
        this.sum=dataInput.readLong();
    }

    @Override
    public String toString() {
        return "DataAccess{" +
                "num='" + num + '\'' +
                ", up='" + up + '\'' +
                ", down='" + down + '\'' +
                ", sum='" + sum + '\'' +
                '}';
    }
}
