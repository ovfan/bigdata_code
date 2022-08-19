package com.fanfan.tutoria.dat02;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @ClassName: TopoSort
 * @Description: TODO 有向无环图
 * @Author: fanfan
 * @DateTime: 2022年08月19日 10时47分
 * @Version: v1.0
 */
public class TopoSort {
    public static void main(String[] args) throws Exception {
        // HashMap
        // {
        //  "A": ["B",C],
        //  "B": ["D","E"],
        //  "C": ["D","E"]
        //  }
        HashMap<String, ArrayList<String>> dag = new HashMap<>();

        ArrayList<String> ANeighbors = new ArrayList<>();
        ANeighbors.add("B");
        ANeighbors.add("C");
        dag.put("A", ANeighbors);

        ArrayList<String> BNeighbors = new ArrayList<>();
        BNeighbors.add("D");
        BNeighbors.add("E");
        // BNeighbors.add("C");
        dag.put("B", BNeighbors);

        ArrayList<String> CNeighbors = new ArrayList<>();
        CNeighbors.add("D");
        CNeighbors.add("E");
        dag.put("C", CNeighbors);

        // 遍历图的算法 -- 拓扑排序
        topologicSort(dag, "A", "A");

    }

    public static void topologicSort(HashMap<String, ArrayList<String>> dag, String vertex, String result) {
        // 递归的退出条件
        if (vertex.equals("D") || vertex.equals("E")) {
            System.out.println(result);
        }
        else {
            // 遍历vertex顶点 指向的所有顶点
            for (String v : dag.get(vertex)){
                topologicSort(dag,v,result + "=>" + v);
            }
        }
    }
}
