package com.hadoop.ss.driver;

import com.hadoop.ss.comparator.SecondarySortComparator;
import com.hadoop.ss.comparator.SecondarySortGroupingComparator;
import com.hadoop.ss.entity.PersonEntity;
import com.hadoop.ss.mapper.SecondarySortMapper;
import com.hadoop.ss.partitioner.PersonPartitioner;
import com.hadoop.ss.reducer.SecondarySortReducer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class SecondarySortDriver extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Secondary Sort <input path> <output path>");
            System.exit(-1);
        }
        return setSecondarySortJob(args);
    }

    private int setSecondarySortJob(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        Job secondarySort = Job.getInstance(getConf());
        secondarySort.setJobName("Secondary Sort");
        secondarySort.setJarByClass(SecondarySortDriver.class);

        FileInputFormat.addInputPath(secondarySort, new Path(args[1]));
        FileOutputFormat.setOutputPath(secondarySort, new Path(args[2]));

        secondarySort.getConfiguration().set("key.value.separator.in.input.line", ",");

        //secondarySort.setInputFormatClass(KeyValueTextInputFormat.class);

        secondarySort.setMapperClass(SecondarySortMapper.class);
        secondarySort.setPartitionerClass(PersonPartitioner.class);
        secondarySort.setSortComparatorClass(SecondarySortComparator.class);
        secondarySort.setGroupingComparatorClass(SecondarySortGroupingComparator.class);
        secondarySort.setReducerClass(SecondarySortReducer.class);

        secondarySort.setOutputKeyClass(Text.class);
        secondarySort.setOutputValueClass(Text.class);

        secondarySort.setMapOutputKeyClass(PersonEntity.class);
        secondarySort.setMapOutputValueClass(Text.class);

        FileSystem fileSystem = FileSystem.newInstance(getConf());
        if (fileSystem.exists(new Path(args[2])))
            fileSystem.delete(new Path(args[2]), true);

        return secondarySort.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new SecondarySortDriver(), args);
    }


}
