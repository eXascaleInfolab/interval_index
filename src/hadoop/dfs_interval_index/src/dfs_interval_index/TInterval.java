package dfs_interval_index;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class TInterval implements WritableComparable<TInterval> {
	Double Start;
	Double End;
	Long Id;
	public TInterval(TInterval copyFrom) {
		Start = copyFrom.Start;
		End = copyFrom.End;
		Id = copyFrom.Id;
	}
	public TInterval(double start, double end, long id) {
		Start = start;
		End = end;
		Id = id;
	}
	public TInterval() {
	}
	public boolean LessThan(TInterval second) {
		if (Start < second.Start) {
			return true;
		} else if (Start == second.Start) {
			if (End < second.End) {
				return true;
			} else if (End == second.End) {
				return Id < second.Id;
			}
			return false;
		}
		return false;
	}
	public boolean Equal(TInterval second) {
		return Start == second.Start && End == second.End && Id == second.Id;
	}
	public int CompareValue(TInterval second) {
		if (Equal(second)) {
			return 0;
		}
		if (LessThan(second)) {
			return -1;
		}
		return 1;
	}
	
    @Override
    public int compareTo(TInterval second) {
    	return CompareValue(second);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
    	Start = in.readDouble();
    	End = in.readDouble();
    	Id = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
    	out.writeDouble(Start);
     	out.writeDouble(End);
    	out.writeLong(Id);
    }
    
    @Override
    public int hashCode() {
      return Start.intValue();
    }
}