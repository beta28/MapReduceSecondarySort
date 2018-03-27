package com.hadoop.ss.comparator;

import com.hadoop.ss.entity.PersonEntity;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class SecondarySortComparator extends WritableComparator {

    public SecondarySortComparator() {
        super(PersonEntity.class, true);
    }

    public int compare(WritableComparable o1, WritableComparable o2) {
        PersonEntity p1 = (PersonEntity) o1;
        PersonEntity p2 = (PersonEntity) o2;
        int compare = p1.getLastName().compareTo(p2.getLastName());
        if (compare == 0)
            compare = p1.getFirstName().compareTo(p2.getFirstName());
        return compare;
    }
}
