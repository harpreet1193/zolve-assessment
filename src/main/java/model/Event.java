package model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class Event {

    private String city;
    private String tenant;

    @Override
    public String toString() {
        return city+"_"+tenant;
    }
}
