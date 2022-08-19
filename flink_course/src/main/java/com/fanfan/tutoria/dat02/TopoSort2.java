package com.fanfan.tutoria.dat02;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @ClassName: TopoSort2
 * @Description: TODO 图的遍历
 * @Author: fanfan
 * @DateTime: 2022年08月19日 11时18分
 * @Version: v1.0
 */
public class TopoSort2 {
    public static void main(String[] args) {
        // A B C D E
        // 使用HashMap数据结构表示 dag,key为 顶点，value为 顶点指向的顶点集合
        HashMap<String, ArrayList<String>> dag = new HashMap<>();

        // 顶点A指向的 集合
        ArrayList<String> ANeighbors = new ArrayList<>();
        ANeighbors.add("B");
        ANeighbors.add("C");
        dag.put("A", ANeighbors);

        // 顶点B指向的 集合
        ArrayList<String> BNeighbors = new ArrayList<>();
        BNeighbors.add("D");
        BNeighbors.add("E");
        dag.put("B", BNeighbors);

        // 顶点C指向的 集合
        ArrayList<String> CNeighbors = new ArrayList<>();
        CNeighbors.add("D");
        CNeighbors.add("E");
        dag.put("C", BNeighbors);

        topoLogicalSort(dag, "A", "A");
    }

    public static void topoLogicalSort(HashMap<String, ArrayList<String>> dag, String vertex, String result) {
        // 如果遍历到的顶点为D 或者 E，直接输出结果
        if (vertex.equals("D") || vertex.equals("E")) {
            System.out.println(result);
        } else {
            ArrayList<String> neighbors = dag.get(vertex);
            for (String v : neighbors) {
                topoLogicalSort(dag, v, result + "=>" + v);
            }
        }
    }
}
