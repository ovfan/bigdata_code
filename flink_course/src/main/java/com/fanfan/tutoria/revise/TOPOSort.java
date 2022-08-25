package com.fanfan.tutoria.revise;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @ClassName: TOPOSort
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年08月25日 09时53分
 * @Version: v1.0
 */
public class TOPOSort {
    public static void main(String[] args) {
        // TODO 实现图的遍历 -- 拓扑排序
        // 1. 定义DAG
        HashMap<String, ArrayList<String>> dag = new HashMap<>();
        // A的Neighbors
        ArrayList<String> ANeighbors = new ArrayList<>();
        ANeighbors.add("B");
        ANeighbors.add("C");
        dag.put("A", ANeighbors);

        // B的Neighbors
        ArrayList<String> BNeighbors = new ArrayList<>();
        BNeighbors.add("D");
        BNeighbors.add("E");
        dag.put("B", BNeighbors);

        ArrayList<String> CNeighbors = new ArrayList<>();
        CNeighbors.add("D");
        CNeighbors.add("E");
        dag.put("C", CNeighbors);

        topoSort(dag, "A", "A");

    }

    public static void topoSort(HashMap<String, ArrayList<String>> dag, String vertex, String result) {
        //
        if (vertex.equals("D") || vertex.equals("E")) {
            System.out.println(result);
        } else {
            for (String v : dag.get(vertex)) {
                topoSort(dag,v,result + "=>" + v);
            }
        }
    }
}
