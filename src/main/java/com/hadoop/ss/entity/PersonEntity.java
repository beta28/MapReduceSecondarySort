package com.hadoop.ss.entity;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PersonEntity implements WritableComparable<PersonEntity> {

    private Text firstName;
    private Text lastName;

    public Text getFirstName() {
        return firstName;
    }

    public Text getLastName() {
        return lastName;
    }

    public void setFirstName(Text firstName) {
        this.firstName = firstName;
    }

    public void setLastName(Text lastName) {
        this.lastName = lastName;
    }

    public PersonEntity() {
        firstName = new Text();
        lastName = new Text();
    }

    private PersonEntity(Text firstName, Text lastName) {
        this.firstName = firstName;
        this.lastName = lastName;
    }

    public PersonEntity(String firstName, String lastName) {
        this(new Text(firstName.trim()), new Text(lastName.trim()));
    }


    @Override
    public int compareTo(PersonEntity personEntity) {
        int compare = lastName.compareTo(personEntity.lastName);
        if (compare == 0) return firstName.compareTo(personEntity.firstName);
        return compare;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        firstName.write(dataOutput);
        lastName.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        firstName.readFields(dataInput);
        lastName.readFields(dataInput);
    }

    @Override
    public String toString() {
        return firstName.toString() + " " + lastName.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((firstName == null) ? 0 : firstName.hashCode());
        result = prime * result + ((lastName == null) ? 0 : lastName.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null) return false;
        if (getClass() != o.getClass()) return false;
        PersonEntity other = (PersonEntity) o;
        if (firstName == null) {
            if (other.firstName != null) return false;
        } else if (!firstName.equals(other.firstName))
            return false;
        if (lastName == null) {
            if (other.lastName != null) return false;
        } else if (!lastName.equals(other.lastName))
            return false;
        return true;
    }
}