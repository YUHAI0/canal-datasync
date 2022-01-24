package com;

import lombok.val;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;

public enum SupportSqlType {

    //
    BIGINT(-5),
    //
    BIT(-7),
    //
    CHAR(1),
    //
    DATE(91),
    //
    DATETIME(93),
    //
    DECIMAL(3),
    //
    FLOAT(7),
    //
    INT(4),
    //
    LONGTEXT(2005),
    //
    MEDIUMINT(4),
    //
    MEDIUMTEXT(2005),
    //
    SMALLINT(5),
    //
    TEXT(2005),
    //
    TIME(92),
    //
    TIMESTAMP(93),
    //
    TINYINT(-6),
    //
    TINYTEXT(2005),
    //
    VARCHAR(12),
    //
    YEAR(12)
    ;

    private int value;

    public static List<SupportSqlType> stringTypes(){
        return Arrays.asList(CHAR, DATE, DATETIME, LONGTEXT, MEDIUMTEXT, TEXT, TIME, VARCHAR);
    }

    SupportSqlType(int value) {
        this.value = value;
    }

    public int getValue() {
        return this.value;
    }

}
