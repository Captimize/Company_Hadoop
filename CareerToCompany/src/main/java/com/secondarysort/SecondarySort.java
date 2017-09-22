package com.secondarysort;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.*;

public class SecondarySort extends Configured implements Tool {

   
public static class compositeKey implements WritableComparable<compositeKey>{
        
        private String dept;
        private String empName;
        private String salary;
        
        public  compositeKey(){
            this.dept = null;
            this.empName = null;
            this.salary = null;
        }
        
        public  compositeKey(String dept, String empName, String salary){
            this.dept = dept;
            this.empName = empName;
            this.salary = salary;
        }

        public void write(DataOutput out) throws IOException{
            out.writeUTF(dept);
            out.writeUTF(empName);
            out.writeUTF(salary);
        }
 
        public void readFields(DataInput in) throws IOException{
            dept = in.readUTF();
            empName = in.readUTF();
            salary = in.readUTF();
        }
        
        @Override
        public String toString(){
            return dept + "-------->" + empName +","+salary;
        }
 
        public int compareTo(compositeKey other){
            int result = dept.compareTo(other.dept);
            if (result == 0){
                result = empName.compareTo(other.empName);
                if (result == 0){
                    result = salary.compareTo(other.salary);
                }
            }
            return result; 
        }
        
        public void setempName(String empName){
            this.empName = empName;
        }
        
        public void setSalary(String salary){
            this.salary = salary;
        }
        
        public void setDept(String dept){
            this.dept = dept;
        }
        
        public String getempName(){
            return empName;
        }
        
        public String getSalary(){
            return salary;
        }
        
        public String getDept(){
            return dept;
        }
        
        @Override
        public int hashCode(){
            return  dept.hashCode();
        }
        
        @Override
        public boolean equals(Object obj){
            compositeKey other = (compositeKey) obj;
            return dept.equals(other.dept) && empName.equals(other.empName) && salary.equals(other.salary); 
        }
    }

    static class empMap extends Mapper<LongWritable, Text, compositeKey, NullWritable> {
        
        @Override
        public void map( LongWritable key,  Text value, Context context) 
                throws IOException,InterruptedException{
        String a = value.toString();
        String[] b = a.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"); 
        String empName = b[0];
        String dept = b[2];
        String salary = b[3];
        if (!(b[0].equals("Name")))
            context.write(new compositeKey(b[2].toString(),b[0].toString(),b[3].toString()),NullWritable.get());
        }
    }
    
    static class empReduce extends Reducer<compositeKey, NullWritable, compositeKey, NullWritable> {
        
        @Override
        public void reduce(compositeKey key, Iterable<NullWritable> values, Context context) 
                throws IOException, InterruptedException {

            for(NullWritable value : values)
                context.write(key,NullWritable.get());
            
        }
    }

    public static class SecondarySortPartitioner 
        extends Partitioner<compositeKey,NullWritable>{
        
        @Override
        public int getPartition(compositeKey key, NullWritable value, int numPartitions){
            return ((key.getDept().hashCode() & Integer.MAX_VALUE) % numPartitions);    
            }
    }
    
    public static class CompkeySortComparator extends WritableComparator {
        protected CompkeySortComparator(){
            super(compositeKey.class,true);
        }
        
        @Override
        public int compare(WritableComparable w1,WritableComparable w2){
            compositeKey ip1 = (compositeKey) w1;
            compositeKey ip2 = (compositeKey) w2;
            
            
            int cmp = ip1.getDept().compareTo(ip2.getDept());
            if (cmp == 0){
                cmp = ip1.getempName().compareTo(ip2.getempName()); //Ascending Order
                if (cmp == 0){
                    return -ip1.getSalary().compareTo(ip2.getSalary()); //Descending Order
                    //If minus is taken out the results will be in ascending order;    
                }
            }
            return cmp;
        }
    }
        
    // Grouping Comparator based on natural key
    public static class groupingComparator extends WritableComparator {
        protected groupingComparator(){
            super(compositeKey.class,true);
        }
        
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            compositeKey key1 = (compositeKey) w1;
            compositeKey key2 = (compositeKey) w2;
            
            return key1.getDept().compareTo(key2.getDept());
        }
    }
    
    public int run(String[] args) throws Exception {
        
        Job job = new Job(getConf());
        job.setJarByClass(getClass());
        job.setJobName("Employee Details .. Sort values in order");
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setMapperClass(empMap.class);
        job.setPartitionerClass(SecondarySortPartitioner.class);
        job.setGroupingComparatorClass(groupingComparator.class);
        job.setSortComparatorClass(CompkeySortComparator.class);
        job.setReducerClass(empReduce.class);

        
        job.setOutputKeyClass(compositeKey.class);
        job.setOutputValueClass(NullWritable.class);
        
        
        return job.waitForCompletion(true)?0 : 1;
    }
    

    public static void main(String[] args) throws Exception{
        int exitcode = ToolRunner.run(new SecondarySort() , args);
        System.exit(exitcode);
    }
}