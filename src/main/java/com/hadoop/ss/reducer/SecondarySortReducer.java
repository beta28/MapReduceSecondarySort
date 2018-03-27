package com.hadoop.ss.reducer;

import com.hadoop.ss.entity.PersonEntity;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SecondarySortReducer extends Reducer<PersonEntity, Text, Text, Text> {
    private static final Logger LOG = LoggerFactory.getLogger(SecondarySortReducer.class);

    public void reduce(PersonEntity key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
        for (Text firstName : value) {
            context.write(new Text(key.getLastName()), new Text(firstName));
            LOG.info("Data emitted by reducer : {} , {}", key.getLastName(), firstName);
        }
    }
}