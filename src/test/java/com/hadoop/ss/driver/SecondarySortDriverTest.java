package com.hadoop.ss.driver;

import com.hadoop.ss.comparator.SecondarySortComparator;
import com.hadoop.ss.comparator.SecondarySortGroupingComparator;
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

public class SecondarySortDriverTest {
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
        mapReduceDriver.setKeyGroupingComparator(new SecondarySortGroupingComparator());
        // TODO : couldn't find test driver for partitioner under mrunit
    }

    @Test
    public void testSort() throws Exception {

        mapReduceDriver.withInput(new LongWritable(0), new Text("Wollock,Peter"));
        mapReduceDriver.withInput(new LongWritable(1), new Text("Pollock,Chen"));
        mapReduceDriver.withInput(new LongWritable(2), new Text("Pollock,Adam"));
        mapReduceDriver.withInput(new LongWritable(3), new Text("Wollock,Andrew"));

        List<Pair<Text, Text>> output = mapReduceDriver.run();
        Assert.assertEquals(4, output.size());
        Assert.assertEquals(new Text("(Pollock, Adam)"), new Text(output.get(0).toString()));
        Assert.assertEquals(new Text("(Pollock, Chen)"), new Text(output.get(1).toString()));
        Assert.assertEquals(new Text("(Wollock, Andrew)"), new Text(output.get(2).toString()));
        Assert.assertEquals(new Text("(Wollock, Peter)"), new Text(output.get(3).toString()));
    }
}
