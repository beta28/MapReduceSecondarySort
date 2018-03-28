package com.hadoop.ss.comparator;

import com.hadoop.ss.entity.PersonEntity;
import com.hadoop.ss.mapper.SecondarySortMapper;
import com.hadoop.ss.reducer.SecondarySortReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class SecondarySortComparatorTest {
    MapDriver<LongWritable, Text, PersonEntity, Text> mapDriver;
    ReduceDriver<PersonEntity, Text, Text, Text> reduceDriver;
    private MapReduceDriver mapReduceDriver;

    /**
     *
     */
    @Before
    public void setUp() {
        SecondarySortMapper mapper = new SecondarySortMapper();
        mapDriver = MapDriver.newMapDriver();
        mapDriver.setMapper(mapper);

        SecondarySortReducer reducer = new SecondarySortReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);

        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
        mapReduceDriver.setKeyOrderComparator(new SecondarySortComparator());
    }

    @Test
    public void testSort() throws Exception {

        mapReduceDriver.withInput(new LongWritable(0), new Text("Wollock,Dixie"));
        mapReduceDriver.withInput(new LongWritable(1), new Text("Pollock,Divito"));
        mapReduceDriver.withInput(new LongWritable(2), new Text("Pollock,Dixey"));

        List<Pair<Text, Text>> output = mapReduceDriver.run();
        Assert.assertEquals(3, output.size());
        Assert.assertEquals(new Text("Pollock"), output.get(0).getFirst());
        Assert.assertEquals(new Text("Pollock"), output.get(1).getFirst());
        Assert.assertEquals(new Text("Wollock"), output.get(2).getFirst());

        Assert.assertEquals(new Text("Divito"), output.get(0).getSecond());
        Assert.assertEquals(new Text("Dixey"), output.get(1).getSecond());
        Assert.assertEquals(new Text("Dixie"), output.get(2).getSecond());
    }
}
