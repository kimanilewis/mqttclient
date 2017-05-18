/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lewis;

import com.sun.org.apache.xalan.internal.xsltc.compiler.util.Type;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author lewie
 */
public class Demo {
    public static void main(String[] args) {

        String number = "31492588744767";
        
        number = number.substring(3);
        System.out.println("> " + number);
        
        int i = Integer.parseInt(number);
        
        //System.out.println("> " + Integer.parseInt(number));
        
      
    }
    
}
