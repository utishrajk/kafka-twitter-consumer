package com.example.kafkatwitterconsumer.model;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.gson.annotations.SerializedName;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
@JsonSerialize
public class Tweet {

    private long id;
    private String text;
    private String lang;
    private User user;

    @SerializedName("created_at")
    private String createdAt;

}
