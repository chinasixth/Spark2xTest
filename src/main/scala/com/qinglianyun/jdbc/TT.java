package com.qinglianyun.jdbc;

/**
 * @ Author ：liuhao
 * @ Company: qinglianyun
 * @ Date   ：Created in 11:01 2018/12/5
 * @
 */
public class TT {
    public static void test(int a, String... args) {
        System.out.println(a);
        for (Object arg : args) {
            System.out.println(arg);
        }
        System.out.println("-=-=-=-=-=-=-=-=-=---------------=-=-=-=-=-");
        test(a, 2, args);
    }

    private static void test(int a, int i, String... args) {
        System.out.println(a + " " + i);
        for (String arg : args) {
            System.out.println(arg);
        }
    }
}
