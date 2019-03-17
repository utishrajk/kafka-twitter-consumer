package com.example.kafkatwitterconsumer.model;

import com.google.gson.annotations.SerializedName;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Tweet {

    private long id;
    private String text;
    private String lang;
    private User user;

    @SerializedName("created_at")
    private String createdAt;

}
