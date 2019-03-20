package com.example.kafkatwitterconsumer.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TweetAccumulator {

    long timestamp;
    int count;
}
