package com.rimi.db;

import org.apache.hadoop.mapred.lib.db.DBWritable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Result implements DBWritable {

    private String word;
    private Integer total;

    public Integer getTotal() {
        return total;
    }

    public String getWord() {
        return word;
    }

    public void setTotal(Integer total) {
        this.total = total;
    }

    public void setWord(String word) {
        this.word = word;
    }

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setString(1,this.word);
        statement.setInt(2,this.total);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.word = resultSet.getString(1);
        this.total = resultSet.getInt(2);
    }
}
