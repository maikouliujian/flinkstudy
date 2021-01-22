package flinks.asynclo;

import lombok.Data;
import lombok.ToString;

/**
 * @author lj
 * @createDate 2019/12/27 16:36
 **/

@Data
@ToString
public class AsyncUser {
    private String id;
    private String username;
    private String password;
    private String phone;

    public AsyncUser(String id, String username, String password) {
        this.id = id;
        this.username = username;
        this.password = password;
    }


}
