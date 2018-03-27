package com.hadoop.ss.mapper;

import com.hadoop.ss.entity.PersonEntity;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SecondarySortMapper extends Mapper<LongWritable, Text, PersonEntity, Text> {

    private static final Logger LOG = LoggerFactory.getLogger(SecondarySortMapper.class);

    /**
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] personValue = value.toString().split(",");
        context.write(new PersonEntity(personValue[1], personValue[0]), new Text(personValue[1]));
        LOG.info("Data emitted from mapper : {} , {}", personValue[0], personValue[1]);
    }
}