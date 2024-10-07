package com.nlu.app.entity;

import lombok.AccessLevel;
import lombok.Data;
import lombok.experimental.FieldDefaults;

import java.time.LocalDateTime;

@Data
@FieldDefaults(makeFinal = true, level = AccessLevel.PUBLIC)
public class Mouse {
    Integer id;
    String name;
    Double price;
    String size;
    Double weight;
    String compatibility;
    String connectionMethod;
    String manufacturer;
    Integer DPI;
    LocalDateTime dateRecorded;
    Double avgRating;
    String imageUrl;
}
