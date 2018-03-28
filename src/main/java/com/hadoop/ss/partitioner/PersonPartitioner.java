package com.hadoop.ss.partitioner;

import com.hadoop.ss.entity.PersonEntity;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PersonPartitioner extends Partitioner<PersonEntity, Text> {

    @Override
    public int getPartition(PersonEntity personEntity, Text text, int numOfReducers) {
        return Math.abs(personEntity.getLastName().hashCode() % numOfReducers);
    }
}