import java.io.*;

import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Vertex implements Writable {
    public long id;                   // the vertex ID
    public Vector<Long> adjacent ;   // the vertex neighbors
    public long centroid;      
    public long size;// the id of the centroid in which this vertex belongs to
    public short depth;               // the BFS depth
    /* ... */
    Vertex ( long id,Vector<Long> adjacent,long centroid,short depth ) {
           this.id = id ; 
         this.adjacent = adjacent; 
         this.centroid = centroid;
         this.depth = depth; 
         size = adjacent.size();
    }
    
  public Vertex() {
    // TODO Auto-generated constructor stub
  }

  public void readFields(DataInput in) throws IOException {
    // TODO Auto-generated method stub
        id =in.readLong();
        centroid =in.readLong();
        depth =in.readShort();
        adjacent=new Vector<Long>();
        size = in.readLong();
          for (int y=0;y<size;y++)
          {
            adjacent.add(in.readLong());
          }
    //    adjacent =in.readVectorUInt();
  }
  public void write(DataOutput out) throws IOException {
    // TODO Auto-generated method stub
         out.writeLong(id);
           out.writeLong(centroid);
           out.writeShort(depth);
           out.writeLong(size);
           for (int i=0;i<size;i++)
           {
            out.writeLong(adjacent.get(i));
           }
        
  }
  
}

public class GraphPartition {
    static Vector<Long> centroids = new Vector<Long>();
    final static short max_depth = 8;
    static short BFS_depth = 0;
  

    public static class FirstMapper extends Mapper<Object,Text,LongWritable,Vertex> {   
      @Override
       public void map(Object key, Text value,
                 Context context ) throws IOException, InterruptedException
         {  
         long centroid;
          Vertex firstcent =new Vertex();
         
      String line = value.toString();
      String[] vertices =  line.split(",");
     
      
      long vertexId = Long.parseLong(vertices[0]);
      
      Vector<Long> adjacent = new Vector<Long>();
      for (int i = 1; i < vertices.length; i++) {
        Long adj=Long.parseLong(vertices[i]);
    
        adjacent.add(adj);
      }
      
            
        
         if(centroids.size() < 10)
         { 
           centroids.add(vertexId);
           centroid =vertexId;
         
           
         }
         else 
         { 
           
           centroid = -1;
           centroids.add(centroid);
          
         }
    
            //Bfs depth
        firstcent = new Vertex(vertexId,adjacent,centroid,(short)0);
        context.write(new LongWritable(vertexId),new Vertex(vertexId,adjacent,centroid,(short)0));

         }
    }
public static class MapperSecondStage extends Mapper<LongWritable, Vertex, LongWritable, Vertex> {
  @Override
    public void map(LongWritable key, Vertex vertex, Context context) throws IOException, InterruptedException {
       Vector<Long> emp = new Vector<Long>();
      context.write(new LongWritable(vertex.id), vertex);
      if(vertex.centroid>0) {
      for(Long n : vertex.adjacent) {
        context.write(new LongWritable(n), new Vertex(n,new Vector<Long>() ,vertex.centroid , BFS_depth));
      }
      }
    } 
    
  }

public static class ReducerSecondStage extends Reducer<LongWritable, Vertex, LongWritable, Vertex> {
  @Override
  public void reduce(LongWritable vertexId, Iterable<Vertex> values, Context context) throws IOException, InterruptedException {
     short min_depth = 1000;
     Vector<Long> emp = new Vector<Long>();
    Vertex m=new Vertex(vertexId.get(),emp,(long)(-1),(short)(0)) ;
    for(Vertex v:values) {
      if(!(v.adjacent.isEmpty()))
      {
        m.adjacent =v.adjacent;
      }
      if(v.centroid > 0 && v.depth < min_depth)
      {
        min_depth= v.depth;
        m.centroid = v.centroid;
      }
    }
     m.depth = min_depth;
     context.write(vertexId,m);
    }
  
  
  }
public static class MapperthirdStage  extends Mapper <LongWritable, Vertex, LongWritable, IntWritable> {
  @Override
  
  public void map(LongWritable centroid,  Vertex value, Context con) throws IOException, InterruptedException {
    con.write(new LongWritable(value.centroid),new IntWritable(1));
  }
  
}
public static class ReducerthirdStage extends Reducer<LongWritable, IntWritable, LongWritable, LongWritable>{

    @Override
    public void reduce(LongWritable centroid, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException
    {
        long m = 0L;
        for (IntWritable v : values)
        {
            m = m + Long.valueOf(v.get()) ;
        }
        context.write(centroid, new LongWritable(m));


}
}
    
    /* ... */

    public static void main ( String[] args ) throws Exception {
        Job job = Job.getInstance();
        job.setJobName("Graph reading");
        
        job.setJarByClass(GraphPartition.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Vertex.class);
        
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Vertex.class);   
        
        job.setInputFormatClass(TextInputFormat.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,FirstMapper.class);
        FileOutputFormat.setOutputPath(job,new Path(args[1]+"/i0"));
        
        /* ... First Map-Reduce job to read the graph */
        job.waitForCompletion(true);

    Path inpath =new Path(args[1]);
    for ( short i = 0; i < max_depth; i++ ) {
            BFS_depth++;
             job = Job.getInstance();
        job.setJobName("Mapping Topography");
        job.setJarByClass(GraphPartition.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Vertex.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Vertex.class);
        job.setMapperClass(MapperSecondStage.class);
        job.setReducerClass(ReducerSecondStage.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        //change
        MultipleInputs.addInputPath(job,new Path(args[1]+"/i0"),SequenceFileInputFormat.class,MapperSecondStage.class);
        FileOutputFormat.setOutputPath(job,new Path(args[1]+"/i"+(i+1)));
        
        job.waitForCompletion(true);
        }
    job = Job.getInstance();
    job.setJobName("Graph Size");
    job.setJarByClass(GraphPartition.class);
    job.setReducerClass(ReducerthirdStage.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(LongWritable.class);

    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(IntWritable.class);
 
        job.setOutputFormatClass(TextOutputFormat.class);
    MultipleInputs.addInputPath(job,new Path(args[1]+"/i8"),SequenceFileInputFormat.class,MapperthirdStage.class);
    FileOutputFormat.setOutputPath(job,new Path(args[2]));
    job.waitForCompletion(true);
    }
}
