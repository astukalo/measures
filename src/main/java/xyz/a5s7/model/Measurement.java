package xyz.a5s7.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;
import java.util.UUID;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Measurement<T> {
    private long timestamp;
    private UUID uuid;
    private Integer partition;
    private T data;
    private boolean isValid;
}
