package com.hadoop.ss.reducer;

import com.hadoop.ss.entity.PersonEntity;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class SecondarySortReducerTest {
    ReduceDriver<PersonEntity, Text, Text, Text> reduceDriver;

    /**
     *
     */
    @Before
    public void setUp() {
        SecondarySortReducer reducer = new SecondarySortReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    /**
     * @throws Exception
     */
    @Test
    public void testReducer() throws Exception {
        List<Text> list = new ArrayList<>();
        list.add(new Text("Divito"));
        list.add(new Text("Dixey"));
        list.add(new Text("Dixie"));
        reduceDriver.setInput(new PersonEntity("Dixey", "Pollock"), list);
        reduceDriver.withOutput(new Text("Pollock"), new Text("Divito"));
        reduceDriver.withOutput(new Text("Pollock"), new Text("Dixey"));
        reduceDriver.withOutput(new Text("Pollock"), new Text("Dixie"));
        reduceDriver.runTest();
    }
}
