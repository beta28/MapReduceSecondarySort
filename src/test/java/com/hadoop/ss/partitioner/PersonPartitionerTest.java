package com.hadoop.ss.partitioner;

import com.hadoop.ss.entity.PersonEntity;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

public class PersonPartitionerTest {

    @Test
    public void testPartitioner() {
        PersonPartitioner personPartitioner = new PersonPartitioner();
        PersonEntity personEntity1 = new PersonEntity("Dixie", "Pollock");
        PersonEntity personEntity2 = new PersonEntity("Dixie", "Wollock");
        Assert.assertEquals(Math.abs(personEntity1.getLastName().hashCode() % 10), personPartitioner.getPartition(personEntity1, new Text(), 10));
        Assert.assertEquals(Math.abs(personEntity2.getLastName().hashCode() % 5), personPartitioner.getPartition(personEntity2, new Text(), 5));
    }
}