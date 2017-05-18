/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author lewie
 */
//package helloservice.endpoint;
import javax.jws.WebMethod;
import javax.jws.WebService;

@WebService(targetNamespace = "http://my.org/ns/")
public class SimpleGenericHTTPSoapClient {

    private String message = new String("Hello, ");

    public void Hello() {
    }

    @WebMethod()
    public String sayHello(String name) {
        return message + name + ".";
    }
    //
// NOTE: the remainder of this deals with reading arguments
//

    /**
     * Main program entry point.
     */
    public static void main(String args[]) {
    }
}

}
